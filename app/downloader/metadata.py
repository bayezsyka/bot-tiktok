
from pydantic import BaseModel


class TikTokMediaItemMetadata(BaseModel):
    position: int
    source_url: str
    media_type: str  # 'video' or 'photo'
    local_path: str | None = None


class TikTokContentMetadata(BaseModel):
    content_type: str  # 'video' or 'photo'
    title: str | None = None
    author: str | None = None
    duration_seconds: int = 0
    items: list[TikTokMediaItemMetadata]
