from datetime import datetime
from typing import Any

from app.webhooks.schemas import ParsedInboundMessage


def parse_inbound_message(payload: dict[str, Any]) -> ParsedInboundMessage | None:
    """
    Defensive parser to extract key message attributes from raw JSON payload.
    Supports flexible/nested structures in case gateway payload schema varies.
    """
    if not isinstance(payload, dict):
        return None

    raw_data = payload.get("data")
    data: dict[str, Any] = raw_data if isinstance(raw_data, dict) else payload
    raw_msg = data.get("message")
    msg: dict[str, Any] = raw_msg if isinstance(raw_msg, dict) else data

    # inbound_message_id
    inbound_id = str(
        data.get("message_id")
        or msg.get("id")
        or msg.get("message_id")
        or data.get("id")
        or payload.get("id")
        or ""
    ).strip()
    if not inbound_id:
        return None

    # sender_number resolution order:
    # 1. sender_phone from data/msg if present and not empty
    # 2. from from data/msg
    # 3. remote_jid from data/msg
    sender_phone_candidates = [
        data.get("sender_phone"),
        msg.get("sender_phone"),
    ]
    sender_phone = next((str(s).strip() for s in sender_phone_candidates if s and str(s).strip()), "")

    # Check if sender_phone is a valid non-LID
    sender = ""
    is_lid = False

    if sender_phone and "@lid" not in sender_phone and not sender_phone.endswith("@lid"):
        sender = sender_phone
    else:
        fallback_candidates = [
            msg.get("from"),
            data.get("from"),
            msg.get("remote_jid"),
            data.get("remote_jid"),
            msg.get("sender"),
            data.get("sender"),
            sender_phone,
        ]
        sender = next((str(s).strip() for s in fallback_candidates if s and str(s).strip()), "")

    if not sender:
        return None

    # If formatted as JID (e.g. 628123456789@s.whatsapp.net), strip suffix safely
    if "@s.whatsapp.net" in sender:
        sender = sender.split("@s.whatsapp.net")[0]
    elif "@" in sender and not sender.endswith("@lid") and not sender.endswith("@g.us"):
        sender = sender.split("@")[0]

    if "@lid" in sender or sender.endswith("@lid"):
        is_lid = True

    # message_text
    text = str(
        msg.get("text")
        or msg.get("body")
        or msg.get("caption")
        or msg.get("message_text")
        or data.get("text")
        or ""
    ).strip()

    # session_id
    session_val = payload.get("session") or data.get("session")
    session_id = None
    if isinstance(session_val, dict):
        session_id = str(session_val.get("id") or "").strip() or None
    if not session_id:
        session_id = str(
            msg.get("session_id")
            or data.get("session_id")
            or payload.get("session_id")
            or ""
        ).strip() or None

    # timestamp (ISO-8601 string or integer/float)
    ts = 0
    ts_val = msg.get("timestamp") or data.get("timestamp") or payload.get("timestamp") or 0
    try:
        if isinstance(ts_val, (int, float)):
            ts = int(ts_val)
        elif isinstance(ts_val, str) and ts_val.strip():
            ts_str = ts_val.strip()
            try:
                ts = int(ts_str)
            except ValueError:
                # Try ISO-8601 parse
                clean_iso = ts_str.replace("Z", "+00:00")
                dt = datetime.fromisoformat(clean_iso)
                ts = int(dt.timestamp())
    except Exception:
        ts = 0

    # is_group
    is_group = bool(
        msg.get("is_group")
        or data.get("is_group")
        or payload.get("is_group")
        or (isinstance(msg.get("from"), str) and "@g.us" in msg.get("from", ""))
        or (isinstance(data.get("from"), str) and "@g.us" in data.get("from", ""))
        or (isinstance(data.get("remote_jid"), str) and "@g.us" in data.get("remote_jid", ""))
    )

    # from_me
    from_me = bool(
        msg.get("from_me")
        or msg.get("is_from_me")
        or data.get("from_me")
        or data.get("is_from_me")
        or payload.get("from_me")
        or payload.get("is_from_me")
        or False
    )

    # Ignore non-text or status/broadcast messages if detected via explicit type without text/caption
    msg_type = str(msg.get("type") or data.get("type") or "text").lower()
    if msg_type not in ("text", "chat", "message") and not text:
        return None

    return ParsedInboundMessage(
        inbound_message_id=inbound_id,
        sender_number=sender,
        message_text=text,
        session_id=session_id,
        timestamp=ts,
        is_group=is_group,
        from_me=from_me,
        is_lid=is_lid,
    )
