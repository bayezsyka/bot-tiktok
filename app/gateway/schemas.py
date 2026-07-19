
from pydantic import BaseModel


class SendMessageRequest(BaseModel):
    type: str = "text"
    to: str
    text: str
    external_reference: str
    session_id: str | None = None


class GatewayMessageResponse(BaseModel):
    status: str = "ok"
    message_id: str | None = None
    data: dict | None = None
