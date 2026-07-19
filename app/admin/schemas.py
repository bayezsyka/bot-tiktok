
from pydantic import BaseModel, Field


class NumberCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=150)
    phone_number: str = Field(..., min_length=10, max_length=30)
    notes: str | None = None


class NumberUpdateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=150)
    phone_number: str = Field(..., min_length=10, max_length=30)
    notes: str | None = None
