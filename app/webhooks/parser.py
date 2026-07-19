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
        msg.get("id")
        or msg.get("message_id")
        or data.get("id")
        or payload.get("id")
        or ""
    ).strip()
    if not inbound_id:
        return None

    # sender_number
    sender = str(
        msg.get("from")
        or msg.get("sender")
        or msg.get("sender_number")
        or data.get("from")
        or ""
    ).strip()
    # If formatted as JID (e.g. 628123456789@s.whatsapp.net), strip suffix
    if "@" in sender:
        sender = sender.split("@")[0]

    # message_text
    text = str(
        msg.get("text")
        or msg.get("body")
        or msg.get("caption")
        or msg.get("message_text")
        or ""
    ).strip()

    # session_id
    session_id = str(
        msg.get("session_id")
        or data.get("session_id")
        or payload.get("session_id")
        or ""
    ).strip() or None

    # timestamp
    ts = 0
    try:
        ts_val = msg.get("timestamp") or data.get("timestamp") or payload.get("timestamp") or 0
        ts = int(ts_val)
    except (ValueError, TypeError):
        pass

    # is_group
    is_group = bool(
        msg.get("is_group")
        or data.get("is_group")
        or payload.get("is_group")
        or (isinstance(msg.get("from"), str) and "@g.us" in msg.get("from", ""))
    )

    # from_me
    from_me = bool(
        msg.get("from_me")
        or data.get("from_me")
        or payload.get("from_me")
        or False
    )

    # Ignore non-text or status/broadcast messages if detected via explicit type
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
    )
