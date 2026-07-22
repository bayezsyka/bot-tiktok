from pydantic import BaseModel


class ParsedInboundMessage(BaseModel):
    inbound_message_id: str
    sender_number: str
    message_text: str
    session_id: str | None = None
    timestamp: int = 0
    is_group: bool = False
    from_me: bool = False
    is_lid: bool = False


class WebhookEventResponse(BaseModel):
    status: str = "ok"
    message: str | None = None
