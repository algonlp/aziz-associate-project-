import asyncio
import os
import time

import httpx
import socket
from twilio.rest import Client
from twilio.twiml.voice_response import Dial, VoiceResponse

from logger_config import get_logger
from services.context_storage import get_call_context, save_call_context
from services.campaign_store import (
    increment_campaign,
    mark_call_transferred,
    update_campaign_contact_by_call_sid,
)
from services.text_utils import strip_non_digits


logger = get_logger("TransferCall")


def _mark_transfer_success(context) -> None:
    setattr(context, "transfer_completed", True)
    setattr(context, "transfer_in_progress", False)
    call_sid = getattr(context, "call_sid", None)
    if call_sid:
        save_call_context(call_sid, context)
    campaign_id = getattr(context, "campaign_id", None)
    if campaign_id and call_sid:
        try:
            if mark_call_transferred(campaign_id, call_sid):
                increment_campaign(campaign_id, "transferred", 1)
            update_campaign_contact_by_call_sid(
                campaign_id,
                call_sid,
                {
                    "transferred": 1,
                    "result": "transferred",
                },
            )
        except Exception as exc:
            logger.warning(
                "Failed to increment transferred counter (campaign_id={}, call_sid={}): {}",
                campaign_id,
                call_sid,
                exc,
            )

def _normalize_e164(number: str) -> str:
    digits = strip_non_digits(number or "")
    if not digits:
        return ""
    if number.strip().startswith("+"):
        return f"+{digits}"
    if digits.startswith("00"):
        return f"+{digits[2:]}"
    return f"+{digits}"


def _channel_name_matches_prefix(channel_name: str, prefix: str) -> bool:
    if not channel_name or not prefix:
        return False
    if channel_name.startswith(prefix):
        return True
    if "/" in channel_name:
        short = channel_name.split("/", 1)[1]
        if short.startswith(prefix):
            return True
    return False

async def _ari_get_channel_name(
    client: httpx.AsyncClient,
    base_url: str,
    auth: tuple,
    channel_id: str,
) -> str:
    url = f"{base_url}/channels/{channel_id}"
    response = await client.get(url, auth=auth)
    if response.status_code != 200:
        return ""
    payload = response.json() or {}
    return payload.get("name") or ""

def _ami_read_response(sock: socket.socket) -> list:
    data = b""
    while True:
        try:
            chunk = sock.recv(1024)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
        if b"\r\n\r\n" in data:
            break
    text = data.decode(errors="ignore")
    return [line for line in text.splitlines() if line.strip()]

def _ami_send_action(sock: socket.socket, action: str, fields: dict) -> list:
    lines = [f"Action: {action}"]
    for key, value in fields.items():
        lines.append(f"{key}: {value}")
    payload = "\r\n".join(lines) + "\r\n\r\n"
    sock.sendall(payload.encode())
    return _ami_read_response(sock)

def _ami_redirect_sync(
    host: str,
    port: int,
    username: str,
    password: str,
    channel_name: str,
    context: str,
    exten: str,
    priority: str = "1",
    timeout_sec: float = 5.0,
) -> tuple[bool, str]:
    if not host or not username or not password or not channel_name:
        return False, "missing AMI configuration or channel name"
    try:
        sock = socket.create_connection((host, port), timeout=timeout_sec)
        sock.settimeout(timeout_sec)
        _ami_read_response(sock)  # consume banner if present
        login_lines = _ami_send_action(
            sock,
            "Login",
            {"Username": username, "Secret": password, "Events": "off"},
        )
        if not login_lines:
            return False, "AMI login failed (no response)"
        if not any(line.startswith("Response: Success") for line in login_lines):
            return False, f"AMI login failed ({'; '.join(login_lines)})"
        redirect_lines = _ami_send_action(
            sock,
            "Redirect",
            {
                "Channel": channel_name,
                "Context": context,
                "Exten": exten,
                "Priority": priority,
            },
        )
        _ami_send_action(sock, "Logoff", {})
        sock.close()
        if not redirect_lines:
            return False, "AMI redirect failed (no response)"
        if any(line.startswith("Response: Success") for line in redirect_lines):
            return True, "AMI redirect succeeded"
        return False, f"AMI redirect failed ({'; '.join(redirect_lines)})"
    except Exception as exc:
        return False, f"AMI redirect exception: {exc}"


async def _find_channel_by_callsid(
    client: httpx.AsyncClient,
    base_url: str,
    auth: tuple,
    call_sid: str,
    prefer_prefix: str = "",
) -> str:
    channels_url = f"{base_url}/channels"
    response = await client.get(channels_url, auth=auth)
    if response.status_code >= 400:
        logger.error(
            "ARI channel lookup failed (status={}, body={})",
            response.status_code,
            response.text,
        )
        response.raise_for_status()
    channels = response.json() or []
    fallback = ""
    for channel in channels[:50]:
        channel_id = channel.get("id")
        channel_name = channel.get("name") or ""
        if not channel_id:
            continue
        var_url = f"{base_url}/channels/{channel_id}/variable"
        var_resp = await client.get(var_url, params={"variable": "TWILIO_CALL_SID"}, auth=auth)
        if var_resp.status_code != 200:
            continue
        value = (var_resp.json() or {}).get("value")
        if value == call_sid:
            if prefer_prefix and _channel_name_matches_prefix(channel_name, prefer_prefix):
                return channel_id
            if not fallback:
                fallback = channel_id
    return fallback

async def _find_channel_by_metadata(
    client: httpx.AsyncClient,
    base_url: str,
    auth: tuple,
    dialed_number: str,
    from_number: str,
    to_number: str,
    prefer_prefix: str = "",
) -> str:
    channels_url = f"{base_url}/channels"
    response = await client.get(channels_url, auth=auth)
    if response.status_code >= 400:
        logger.error(
            "ARI channel lookup failed (status={}, body={})",
            response.status_code,
            response.text,
        )
        response.raise_for_status()
    channels = response.json() or []
    fallback = ""
    for channel in channels[:100]:
        channel_id = channel.get("id")
        channel_name = channel.get("name") or ""
        dialplan = channel.get("dialplan") or {}
        exten = str(dialplan.get("exten") or "")
        context = str(dialplan.get("context") or "")
        caller = channel.get("caller") or {}
        connected = channel.get("connected") or {}
        caller_number = str(caller.get("number") or "")
        caller_name = str(caller.get("name") or "")
        connected_number = str(connected.get("number") or "")

        if dialed_number and exten != dialed_number:
            continue
        if from_number and from_number not in (caller_number, caller_name):
            continue
        if to_number and to_number not in (connected_number, exten):
            continue
        if context and context not in ("send-a1r", "from-twilio"):
            continue
        if channel_id:
            if prefer_prefix and _channel_name_matches_prefix(channel_name, prefer_prefix):
                return channel_id
            if not fallback:
                fallback = channel_id
    return fallback


async def transfer_call(context, args):
    require_confirm = os.getenv("REQUIRE_TRANSFER_CONFIRMATION", "false").lower() == "true"
    call_sid = getattr(context, "call_sid", None)
    if call_sid:
        cached = get_call_context(call_sid)
        if getattr(cached, "transfer_completed", False):
            logger.info("Transfer already completed (CallSid={}); skipping.", call_sid)
            return "Transfer already completed."
        if getattr(cached, "transfer_started", False):
            logger.info("Transfer already started (CallSid={}); skipping.", call_sid)
            return "Transfer already in progress."
        if require_confirm and not getattr(cached, "transfer_user_confirmed", False) and not getattr(context, "transfer_user_confirmed", False):
            logger.warning("Transfer blocked: missing explicit confirmation (CallSid={}).", call_sid)
            return "Transfer blocked: missing explicit confirmation."
    if getattr(context, "transfer_completed", False):
        logger.info("Transfer already completed; skipping duplicate request.")
        return "Transfer already completed."
    if getattr(context, "transfer_started", False):
        logger.info("Transfer already started; skipping duplicate request.")
        return "Transfer already in progress."
    if require_confirm and not getattr(context, "transfer_user_confirmed", False):
        logger.warning("Transfer blocked: missing explicit confirmation.")
        return "Transfer blocked: missing explicit confirmation."
    setattr(context, "transfer_in_progress", True)
    setattr(context, "transfer_started", True)
    if call_sid:
        save_call_context(call_sid, context)

    ari_channel_id = getattr(context, "ari_channel_id", None)
    if not ari_channel_id:
        wait_s = float(os.getenv("ARI_TRANSFER_TIMEOUT", "0") or "0")
        deadline = time.monotonic() + max(wait_s, 0.0)
        while time.monotonic() < deadline and not ari_channel_id:
            cached_context = get_call_context(getattr(context, "call_sid", ""))
            ari_channel_id = getattr(cached_context, "ari_channel_id", None) if cached_context else None
            if ari_channel_id:
                context.ari_channel_id = ari_channel_id
                break
            await asyncio.sleep(0.2)
    args = args or {}
    configured_transfer = os.getenv("TRANSFER_NUMBER", "").strip()
    default_transfer = os.getenv("DEFAULT_TRANSFER_NUMBER", "")
    transfer_number = (
        str(args.get("transfer_number") or "").strip()
        or getattr(context, "transfer_number", "")
        or configured_transfer
        or default_transfer
    )
    transfer_e164 = _normalize_e164(transfer_number)
    transfer_extension = strip_non_digits(transfer_number)
    logger.info(
        "Transfer target resolved (call_sid={}, target_raw={}, target_e164={})",
        getattr(context, "call_sid", None),
        transfer_number,
        transfer_e164,
    )
    if not transfer_extension:
        logger.error("Transfer failed: missing transfer target.")
        setattr(context, "transfer_in_progress", False)
        setattr(context, "transfer_started", False)
        if getattr(context, "call_sid", None):
            save_call_context(context.call_sid, context)
        return "Error transferring call: missing transfer target."

    base_url = (os.getenv("ASTERISK_ARI_BASE_URL") or "").rstrip("/")
    username = os.getenv("ASTERISK_ARI_USERNAME", "")
    password = os.getenv("ASTERISK_ARI_PASSWORD", "")
    context_name = os.getenv("ASTERISK_TRANSFER_CONTEXT", "call-transfer")
    twilio_fallback = os.getenv("TRANSFER_ENABLE_TWILIO_FALLBACK", "false").lower() == "true"

    def _mark_transfer_failed() -> None:
        setattr(context, "transfer_in_progress", False)
        setattr(context, "transfer_started", False)
        if getattr(context, "call_sid", None):
            save_call_context(context.call_sid, context)

    async def _twilio_transfer(reason: str) -> str:
        call_sid = getattr(context, "call_sid", None)
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        if not call_sid:
            logger.error("Twilio transfer failed: missing CallSid. Reason: {}", reason)
            return f"Error transferring call: missing CallSid ({reason})."
        if not account_sid or not auth_token:
            logger.error("Twilio transfer failed: missing credentials. Reason: {}", reason)
            return f"Error transferring call: missing Twilio credentials ({reason})."
        if not transfer_e164:
            logger.error("Twilio transfer failed: invalid transfer number. Reason: {}", reason)
            return f"Error transferring call: invalid transfer number ({reason})."
        caller_id = (
            os.getenv("TRANSFER_CALLER_ID", "")
            or os.getenv("APP_NUMBER", "")
            or getattr(context, "twilio_to", "")
        )
        caller_id = _normalize_e164(caller_id) if caller_id else ""
        response = VoiceResponse()
        dial = Dial(callerId=caller_id) if caller_id else Dial()
        dial.number(transfer_e164)
        response.append(dial)
        try:
            client = Client(account_sid, auth_token)
            call = await asyncio.to_thread(client.calls(call_sid).fetch)
            if call.status not in ("in-progress", "ringing", "queued"):
                logger.info(
                    "Twilio transfer skipped; call not in progress (status={}, reason={}).",
                    call.status,
                    reason,
                )
                setattr(context, "transfer_in_progress", False)
                setattr(context, "transfer_started", False)
                if getattr(context, "call_sid", None):
                    save_call_context(context.call_sid, context)
                return "Call not in progress; transfer skipped."
            await asyncio.to_thread(client.calls(call_sid).update, twiml=str(response))
            logger.info("Twilio transfer succeeded (call_sid={}). Reason: {}", call_sid, reason)
            _mark_transfer_success(context)
            return "Call transferred."
        except Exception as exc:
            logger.exception("Twilio transfer exception: {}", exc)
            setattr(context, "transfer_in_progress", False)
            setattr(context, "transfer_started", False)
            if getattr(context, "call_sid", None):
                save_call_context(context.call_sid, context)
            return f"Error transferring call: {str(exc)}"

    try:
        if not base_url or not username or not password:
            logger.warning("ARI transfer skipped: missing ARI configuration.")
            if twilio_fallback:
                return await _twilio_transfer("missing ARI configuration")
            _mark_transfer_failed()
            return "Error transferring call: missing ARI configuration."
        async with httpx.AsyncClient(timeout=10.0) as client:
            prefer_prefix = os.getenv("ASTERISK_TRANSFER_CHANNEL_PREFIX", "PJSIP/a1r-").strip()
            if ari_channel_id and prefer_prefix:
                channel_name = await _ari_get_channel_name(
                    client,
                    base_url,
                    (username, password),
                    ari_channel_id,
                )
                if not _channel_name_matches_prefix(channel_name, prefer_prefix):
                    ari_channel_id = ""
            if not ari_channel_id and getattr(context, "call_sid", None):
                ari_channel_id = await _find_channel_by_callsid(
                    client,
                    base_url,
                    (username, password),
                    context.call_sid,
                    prefer_prefix=prefer_prefix,
                )
                if ari_channel_id:
                    context.ari_channel_id = ari_channel_id
                    save_call_context(context.call_sid, context)
                    url = f"{base_url}/channels/{ari_channel_id}/continue"
                    logger.info(
                        "Resolved ARI channel id via CallSid (call_sid={}, channel_id={})",
                    context.call_sid,
                    ari_channel_id,
                    )
            if not ari_channel_id:
                dialed_number = strip_non_digits(getattr(context, "dialed_number", "") or "")
                from_number = strip_non_digits(getattr(context, "twilio_from", "") or "")
                to_number = strip_non_digits(getattr(context, "twilio_to", "") or "")
                ari_channel_id = await _find_channel_by_metadata(
                    client,
                    base_url,
                    (username, password),
                    dialed_number,
                    from_number,
                    to_number,
                    prefer_prefix=prefer_prefix,
                )
                if ari_channel_id and getattr(context, "call_sid", None):
                    context.ari_channel_id = ari_channel_id
                    save_call_context(context.call_sid, context)
                    logger.info(
                        "Resolved ARI channel id via metadata (call_sid={}, channel_id={})",
                        context.call_sid,
                        ari_channel_id,
                    )
            if not ari_channel_id:
                logger.error("Transfer failed: missing ARI channel id.")
                if twilio_fallback:
                    return await _twilio_transfer("missing ARI channel id")
                _mark_transfer_failed()
                return "Error transferring call: missing ARI channel id."
            channel_name = await _ari_get_channel_name(client, base_url, (username, password), ari_channel_id)
            url = f"{base_url}/channels/{ari_channel_id}/continue"
            params = {"context": context_name, "extension": transfer_extension, "priority": "1"}
            logger.info(
                "Requesting ARI transfer (channel_id={}, channel_name={}, context={}, extension={})",
                ari_channel_id,
                channel_name or "<unknown>",
                context_name,
                transfer_extension,
            )
            response = await client.post(url, params=params, auth=(username, password))
            if response.status_code >= 400:
                logger.error(
                    "ARI transfer failed (status={}, body={})",
                    response.status_code,
                    response.text,
                )
                response.raise_for_status()
        logger.info("ARI transfer succeeded (channel_id={}).", ari_channel_id)
        _mark_transfer_success(context)
        return "Call transferred."
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response else None
        if status == 409:
            ami_host = os.getenv("ASTERISK_AMI_HOST", "")
            ami_port = int(os.getenv("ASTERISK_AMI_PORT", "5038") or "5038")
            ami_user = os.getenv("ASTERISK_AMI_USERNAME", "")
            ami_pass = os.getenv("ASTERISK_AMI_PASSWORD", "")
            ami_timeout = float(os.getenv("ASTERISK_AMI_TIMEOUT_SEC", "2.5") or "2.5")
            recovery_mode = os.getenv("ARI_409_RECOVERY_MODE", "ami_direct").strip().lower()
            logger.warning(
                "ARI transfer conflict (channel not in Stasis). Recovery mode={}.",
                recovery_mode,
            )
            handoff_context = os.getenv("ASTERISK_ARI_HANDOFF_CONTEXT", "ari-handoff")
            if ami_host and ami_user and ami_pass:
                channel_name = ""
                async with httpx.AsyncClient(timeout=5.0) as ami_client:
                    channel_name = await _ari_get_channel_name(
                        ami_client,
                        base_url,
                        (username, password),
                        ari_channel_id,
                    )
                if channel_name:
                    if recovery_mode != "ami_handoff":
                        ok, msg = await asyncio.to_thread(
                            _ami_redirect_sync,
                            ami_host,
                            ami_port,
                            ami_user,
                            ami_pass,
                            channel_name,
                            context_name,
                            transfer_extension,
                            "1",
                            ami_timeout,
                        )
                        logger.info("AMI direct redirect result: {} ({})", ok, msg)
                        if ok:
                            _mark_transfer_success(context)
                            return "Call transferred."

                    ok, msg = await asyncio.to_thread(
                        _ami_redirect_sync,
                        ami_host,
                        ami_port,
                        ami_user,
                        ami_pass,
                        channel_name,
                        handoff_context,
                        transfer_extension,
                        "1",
                        ami_timeout,
                    )
                    logger.info("AMI redirect result: {} ({})", ok, msg)
                    if ok:
                        wait_step = float(os.getenv("AMI_REDIRECT_WAIT_STEP_SEC", "0.15") or "0.15")
                        retry_for = float(os.getenv("ARI_AFTER_AMI_RETRY_SEC", "2.0") or "2.0")
                        deadline = time.monotonic() + max(0.2, retry_for)
                        last_status = None
                        last_body = ""
                        while time.monotonic() < deadline:
                            async with httpx.AsyncClient(timeout=10.0) as retry_client:
                                retry = await retry_client.post(
                                    url, params=params, auth=(username, password)
                                )
                            if retry.status_code < 400:
                                logger.info("ARI transfer succeeded after AMI redirect.")
                                _mark_transfer_success(context)
                                return "Call transferred."
                            last_status = retry.status_code
                            last_body = retry.text
                            if retry.status_code != 409:
                                break
                            await asyncio.sleep(max(0.05, wait_step))
                        logger.error(
                            "ARI transfer after AMI redirect failed (status={}, body={})",
                            last_status,
                            last_body,
                        )
            if twilio_fallback:
                logger.warning("AMI/ARI retry did not complete. Falling back to Twilio transfer by configuration.")
                return await _twilio_transfer("ARI 409 conflict (channel not in Stasis)")
            _mark_transfer_failed()
            return "Error transferring call: ARI conflict and AMI handoff failed."
        logger.exception("ARI transfer HTTP error: {}", exc)
        if twilio_fallback:
            return await _twilio_transfer("ARI HTTP error")
        _mark_transfer_failed()
        return f"Error transferring call: {str(exc)}"
    except Exception as exc:
        logger.exception("ARI transfer exception: {}", exc)
        setattr(context, "transfer_in_progress", False)
        setattr(context, "transfer_started", False)
        if getattr(context, "call_sid", None):
            save_call_context(context.call_sid, context)
        if twilio_fallback:
            return await _twilio_transfer("ARI transfer exception")
        return f"Error transferring call: {str(exc)}"
