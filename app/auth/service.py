import uuid

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

ph = PasswordHasher()


def hash_password(password: str) -> str:
    return ph.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return ph.verify(password_hash, password)
    except (VerifyMismatchError, Exception):
        return False


def rotate_session(request_session: dict, admin_id: int) -> None:
    """Clear old session data and set new session identifier upon successful login."""
    request_session.clear()
    request_session["admin_id"] = str(admin_id)
    request_session["session_id"] = str(uuid.uuid4())
