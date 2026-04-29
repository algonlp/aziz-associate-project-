import asyncio
import os
import time

from twilio.rest import Client

from logger_config import get_logger

logger = get_logger("EndCall")

async def end_call(context, args):
    # Retrieve the Twilio credentials from environment variables
    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    client = Client(account_sid, auth_token)
    call_sid = getattr(context, "call_sid", None)
    if not call_sid:
        logger.warning("end_call skipped: call_sid missing in context.")
        return "Call SID missing; unable to end the call."

    # Fetch the call
    try:
        call = client.calls(call_sid).fetch()
    except Exception as exc:
        logger.error("Twilio fetch failed for {}: {}", call_sid, exc)
        return f"Unable to fetch call status for {call_sid}."

    # Check if the call is already completed
    if call.status in ['completed', 'failed', 'busy', 'no-answer', 'canceled']:
        return f"Call already ended with status: {call.status}"

    # Wait until farewell audio has time to play before hanging up.
    min_grace = float(os.getenv("END_CALL_MIN_GRACE_SEC", os.getenv("END_CALL_GRACE_SEC", "1.2")))
    base_max_wait = float(os.getenv("END_CALL_MAX_WAIT_SEC", "8.0"))
    absolute_max_wait = float(os.getenv("END_CALL_ABSOLUTE_MAX_WAIT_SEC", "20.0"))
    silence_wait = float(os.getenv("END_CALL_SILENCE_WAIT_SEC", "0.35"))
    poll = float(os.getenv("END_CALL_WAIT_POLL_SEC", "0.05"))
    words_per_sec = float(os.getenv("END_CALL_WORDS_PER_SEC", "2.8"))
    expected_cap = float(os.getenv("END_CALL_EXPECTED_SPEECH_MAX_SEC", "12.0"))
    expected_buffer = float(os.getenv("END_CALL_EXPECTED_BUFFER_SEC", "0.45"))
    started = time.monotonic()
    saw_tts = False
    farewell_text = ""
    if isinstance(args, dict):
        farewell_text = str(args.get("farewell") or "").strip()
    target_text = farewell_text or str(getattr(context, "last_assistant_message", "") or "").strip()
    target_words = len([w for w in target_text.split() if w.strip()])
    expected_speech_sec = 0.0
    if target_words >= 4 and words_per_sec > 0:
        expected_speech_sec = min(expected_cap, (target_words / words_per_sec) + expected_buffer)
    required_audio_sec = max(min_grace, expected_speech_sec)
    max_wait = min(absolute_max_wait, max(base_max_wait, required_audio_sec + 1.5))
    speech_started_at = None

    while True:
        now = time.monotonic()
        elapsed = now - started
        if elapsed >= max_wait:
            break
        if elapsed < min_grace:
            await asyncio.sleep(max(0.01, poll))
            continue

        tts_speaking = bool(getattr(context, "tts_speaking", False))
        last_tts_start = getattr(context, "last_tts_start_ts", None)
        last_tts_audio = getattr(context, "last_tts_audio_ts", None)
        if last_tts_start and (speech_started_at is None or float(last_tts_start) > float(speech_started_at)):
            speech_started_at = float(last_tts_start)
        if last_tts_audio and (speech_started_at is None):
            speech_started_at = float(last_tts_audio)
        if tts_speaking:
            saw_tts = True
            await asyncio.sleep(max(0.01, poll))
            continue
        if last_tts_audio and (now - float(last_tts_audio)) < silence_wait:
            saw_tts = True
            await asyncio.sleep(max(0.01, poll))
            continue

        last_stop = getattr(context, "last_tts_stop_ts", None)
        if speech_started_at and (now - float(speech_started_at)) < required_audio_sec:
            await asyncio.sleep(max(0.01, poll))
            continue
        if saw_tts:
            if not last_stop:
                await asyncio.sleep(max(0.01, poll))
                continue
            if (now - float(last_stop)) < silence_wait:
                await asyncio.sleep(max(0.01, poll))
                continue
            break

        # If we expect a spoken farewell but TTS has not started yet, allow extra time.
        if target_words >= 4:
            last_assistant = getattr(context, "last_assistant_utterance_ts", None)
            if last_assistant and (now - float(last_assistant)) < required_audio_sec:
                await asyncio.sleep(max(0.01, poll))
                continue
        break

    try:
        call = client.calls(call_sid).fetch()
        if call.status in ['completed', 'failed', 'busy', 'no-answer', 'canceled']:
            return f"Call already ended with status: {call.status}"
        call = client.calls(call_sid).update(status="completed")
        return f"Call ended successfully. Final status: {call.status}"
    except Exception as exc:
        logger.error("Twilio update failed for {}: {}", call_sid, exc)
        return f"Failed to end call for {call_sid}: {exc}"
