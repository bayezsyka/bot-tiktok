
from fastapi import HTTPException, Request, status
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.config import get_settings


def get_serializer() -> URLSafeTimedSerializer:
    settings = get_settings()
    return URLSafeTimedSerializer(settings.APP_SECRET, salt="farros-csrf-salt")


def generate_csrf_token(session_id: str) -> str:
    """Generate a signed CSRF token tied to the user's session identifier."""
    serializer = get_serializer()
    return serializer.dumps({"sid": session_id})


def verify_csrf_token(token: str | None, session_id: str, max_age: int = 86400) -> bool:
    """Verify that the CSRF token is valid, unexpired, and belongs to the active session."""
    if not token or not session_id:
        return False
    serializer = get_serializer()
    try:
        data = serializer.loads(token, max_age=max_age)
        return isinstance(data, dict) and data.get("sid") == session_id
    except (BadSignature, SignatureExpired):
        return False


async def validate_csrf_request(request: Request) -> None:
    """FastAPI dependency or check to enforce valid CSRF on state-changing requests."""
    session_id = request.session.get("session_id") or request.session.get("admin_id")
    if not session_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Session expired or invalid")

    # Check header first (for HTMX), then form field
    token = request.headers.get("X-CSRF-Token")
    if not token and request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
        try:
            form = await request.form()
            token_val = form.get("_csrf_token")
            if isinstance(token_val, str):
                token = token_val
        except Exception:
            pass

    if not verify_csrf_token(token, str(session_id)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF token validation failed")
