import hashlib
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from services.redis_client import get_redis_client


redis_client = get_redis_client()

CAMPAIGN_KEY_PREFIX = "campaign:"


def _campaign_key(campaign_id: str) -> str:
    return f"{CAMPAIGN_KEY_PREFIX}{campaign_id}"


def _finalized_key(campaign_id: str) -> str:
    return f"{CAMPAIGN_KEY_PREFIX}{campaign_id}:finalized"


def _transferred_key(campaign_id: str) -> str:
    return f"{CAMPAIGN_KEY_PREFIX}{campaign_id}:transferred"


def _contacts_index_key(campaign_id: str) -> str:
    return f"{CAMPAIGN_KEY_PREFIX}{campaign_id}:contacts"


def _contact_key(campaign_id: str, contact_id: str) -> str:
    return f"{CAMPAIGN_KEY_PREFIX}{campaign_id}:contact:{contact_id}"


def _call_to_contact_key(campaign_id: str, call_sid: str) -> str:
    return f"{CAMPAIGN_KEY_PREFIX}{campaign_id}:call_contact:{call_sid}"


def _campaign_ttl() -> int:
    if (os.getenv("CAMPAIGN_STORE_PERSIST", "true") or "true").strip().lower() in {"1", "true", "yes"}:
        return 0
    return int(os.getenv("CAMPAIGN_TTL_SEC", "0"))


def _set_ttl_if_needed(*keys: str) -> None:
    ttl_sec = _campaign_ttl()
    if ttl_sec <= 0:
        return
    for key in keys:
        if key:
            redis_client.expire(key, ttl_sec)


def _contact_id_from_phone(phone_number: str) -> str:
    raw = str(phone_number or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    fingerprint = digits or raw.lower()
    return hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()


def create_campaign(campaign_id: str, total_targets: int, metadata: Optional[Dict[str, Any]] = None) -> None:
    now = datetime.utcnow().isoformat()
    payload: Dict[str, Any] = {
        "id": campaign_id,
        "status": "running",
        "created_at": now,
        "total_targets": int(total_targets),
        "initiated": 0,
        "completed": 0,
        "success": 0,
        "declined": 0,
        "failed_initiate": 0,
        "answered": 0,
        "transferred": 0,
    }
    if metadata:
        for key, value in metadata.items():
            if value is None:
                continue
            payload[key] = value

    redis_client.hset(_campaign_key(campaign_id), mapping=payload)

    _set_ttl_if_needed(
        _campaign_key(campaign_id),
        _finalized_key(campaign_id),
        _transferred_key(campaign_id),
        _contacts_index_key(campaign_id),
    )


def get_campaign(campaign_id: str) -> Optional[Dict[str, Any]]:
    data = redis_client.hgetall(_campaign_key(campaign_id))
    if not data:
        return None
    decoded: Dict[str, Any] = {}
    for key, value in data.items():
        decoded_key = key.decode("utf-8")
        decoded_val = value.decode("utf-8")
        decoded[decoded_key] = decoded_val

    int_fields = {
        "total_targets",
        "initiated",
        "completed",
        "success",
        "declined",
        "failed_initiate",
        "answered",
        "transferred",
    }
    for field in int_fields:
        if field in decoded:
            try:
                decoded[field] = int(decoded[field])
            except ValueError:
                decoded[field] = 0
    return decoded


def list_campaigns(limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    campaign_ids: List[str] = []
    seen: set[str] = set()
    for raw_key in redis_client.scan_iter(match=f"{CAMPAIGN_KEY_PREFIX}*"):
        key = raw_key.decode("utf-8")
        if not key.startswith(CAMPAIGN_KEY_PREFIX):
            continue
        suffix = key[len(CAMPAIGN_KEY_PREFIX):]
        # Ignore helper keys like campaign:<id>:finalized and campaign:<id>:transferred
        if ":" in suffix:
            continue
        if not suffix or suffix in seen:
            continue
        seen.add(suffix)
        campaign_ids.append(suffix)

    snapshots: List[Dict[str, Any]] = []
    for campaign_id in campaign_ids:
        snapshot = get_campaign(campaign_id)
        if snapshot:
            snapshots.append(snapshot)

    snapshots.sort(
        key=lambda item: str(item.get("created_at") or item.get("dialing_completed_at") or ""),
        reverse=True,
    )
    safe_offset = max(0, int(offset))
    safe_limit = int(limit or 0)
    if safe_limit <= 0:
        return snapshots[safe_offset:]
    return snapshots[safe_offset:safe_offset + safe_limit]


def upsert_campaign_contact(
    campaign_id: str,
    phone_number: str,
    name: str = "",
    fields: Optional[Dict[str, Any]] = None,
) -> str:
    phone = str(phone_number or "").strip()
    if not phone:
        return ""
    contact_id = _contact_id_from_phone(phone)
    contact_key = _contact_key(campaign_id, contact_id)
    mapping: Dict[str, Any] = {
        "contact_id": contact_id,
        "phone_number": phone,
    }
    clean_name = str(name or "").strip()
    if clean_name:
        mapping["name"] = clean_name
    if fields:
        for key, value in fields.items():
            if value is None:
                continue
            mapping[key] = value
    redis_client.hset(contact_key, mapping=mapping)
    redis_client.sadd(_contacts_index_key(campaign_id), contact_id)
    _set_ttl_if_needed(contact_key, _contacts_index_key(campaign_id))
    return contact_id


def link_call_to_contact(
    campaign_id: str,
    call_sid: str,
    phone_number: str,
    name: str = "",
) -> str:
    if not call_sid:
        return ""
    contact_id = upsert_campaign_contact(campaign_id, phone_number, name=name)
    if not contact_id:
        return ""
    call_key = _call_to_contact_key(campaign_id, call_sid)
    redis_client.set(call_key, contact_id)
    _set_ttl_if_needed(call_key)
    redis_client.hset(
        _contact_key(campaign_id, contact_id),
        mapping={"call_sid": call_sid},
    )
    _set_ttl_if_needed(_contact_key(campaign_id, contact_id))
    return contact_id


def update_campaign_contact_by_call_sid(
    campaign_id: str,
    call_sid: str,
    fields: Dict[str, Any],
) -> bool:
    if not campaign_id or not call_sid or not fields:
        return False
    call_key = _call_to_contact_key(campaign_id, call_sid)
    raw_contact_id = redis_client.get(call_key)
    if not raw_contact_id:
        return False
    contact_id = (
        raw_contact_id.decode("utf-8")
        if isinstance(raw_contact_id, (bytes, bytearray))
        else str(raw_contact_id)
    )
    if not contact_id:
        return False
    mapping: Dict[str, Any] = {}
    for key, value in fields.items():
        if value is None:
            continue
        mapping[key] = value
    if not mapping:
        return False
    redis_client.hset(_contact_key(campaign_id, contact_id), mapping=mapping)
    _set_ttl_if_needed(_contact_key(campaign_id, contact_id), call_key)
    return True


def list_campaign_contacts(campaign_id: str, limit: int = 0, offset: int = 0) -> List[Dict[str, Any]]:
    contact_ids_raw = redis_client.smembers(_contacts_index_key(campaign_id)) or set()
    contact_ids: List[str] = []
    for raw_id in contact_ids_raw:
        contact_id = raw_id.decode("utf-8") if isinstance(raw_id, (bytes, bytearray)) else str(raw_id)
        if contact_id:
            contact_ids.append(contact_id)

    contacts: List[Dict[str, Any]] = []
    int_fields = {"duration_sec", "answered", "transferred"}
    for contact_id in contact_ids:
        data = redis_client.hgetall(_contact_key(campaign_id, contact_id))
        if not data:
            continue
        decoded: Dict[str, Any] = {}
        for key, value in data.items():
            decoded_key = key.decode("utf-8")
            decoded_val = value.decode("utf-8")
            decoded[decoded_key] = decoded_val
        for field in int_fields:
            if field in decoded:
                try:
                    decoded[field] = int(decoded[field])
                except ValueError:
                    decoded[field] = 0
        contacts.append(decoded)

    contacts.sort(
        key=lambda item: str(
            item.get("completed_at")
            or item.get("initiated_at")
            or item.get("updated_at")
            or ""
        ),
        reverse=True,
    )
    safe_offset = max(0, int(offset))
    if limit and int(limit) > 0:
        safe_limit = int(limit)
        return contacts[safe_offset:safe_offset + safe_limit]
    return contacts[safe_offset:]


def set_campaign_fields(campaign_id: str, fields: Dict[str, Any]) -> None:
    if not fields:
        return
    redis_client.hset(_campaign_key(campaign_id), mapping=fields)


def increment_campaign(campaign_id: str, field: str, amount: int = 1) -> int:
    return int(redis_client.hincrby(_campaign_key(campaign_id), field, amount))


def mark_call_finalized(campaign_id: str, call_sid: str) -> bool:
    return redis_client.sadd(_finalized_key(campaign_id), call_sid) == 1


def mark_call_transferred(campaign_id: str, call_sid: str) -> bool:
    return redis_client.sadd(_transferred_key(campaign_id), call_sid) == 1
