import hashlib
import hmac
import time

from app.webhooks.signature import verify_webhook_signature


def test_verify_webhook_signature_valid() -> None:
    secret = "my-secret-key-123"
    timestamp = str(int(time.time()))
    body = b'{"event":"message.inbound","data":{"id":"123"}}'

    msg = f"{timestamp}.{body.decode('utf-8')}".encode()
    signature = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

    headers = {
        "x-fwag-event": "message.inbound",
        "x-fwag-event-id": "evt-001",
        "x-fwag-timestamp": timestamp,
        "x-fwag-signature": signature,
    }
    assert verify_webhook_signature(headers, body, secret, tolerance_seconds=300) is True


def test_verify_webhook_signature_wrong_secret() -> None:
    secret = "my-secret-key-123"
    wrong_secret = "wrong-secret-key"
    timestamp = str(int(time.time()))
    body = b'{"hello":"world"}'

    msg = f"{timestamp}.{body.decode('utf-8')}".encode()
    signature = hmac.new(wrong_secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

    headers = {
        "x-fwag-event": "message.inbound",
        "x-fwag-event-id": "evt-002",
        "x-fwag-timestamp": timestamp,
        "x-fwag-signature": signature,
    }
    assert verify_webhook_signature(headers, body, secret, tolerance_seconds=300) is False


def test_verify_webhook_signature_expired_timestamp() -> None:
    secret = "my-secret-key-123"
    # timestamp 10 minutes in the past
    timestamp = str(int(time.time()) - 600)
    body = b'{"hello":"world"}'

    msg = f"{timestamp}.{body.decode('utf-8')}".encode()
    signature = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

    headers = {
        "x-fwag-event": "message.inbound",
        "x-fwag-event-id": "evt-003",
        "x-fwag-timestamp": timestamp,
        "x-fwag-signature": signature,
    }
    # Tolerance is 300s (5m), should fail
    assert verify_webhook_signature(headers, body, secret, tolerance_seconds=300) is False


def test_verify_webhook_signature_missing_headers() -> None:
    headers = {"x-fwag-event": "message.inbound"}
    assert verify_webhook_signature(headers, b"{}", "secret") is False
