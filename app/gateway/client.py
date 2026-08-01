import asyncio
import logging
import re
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.gateway.exceptions import (
    GatewayError,
    GatewayNetworkError,
    GatewayRateLimitError,
    GatewayResponseError,
    GatewayTimeoutError,
)
from app.gateway.schemas import GatewayMessageResponse

logger = logging.getLogger(__name__)

IDEMPOTENCY_KEY_REGEX = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")


GATEWAY_MEDIA_TYPE_MAP = {
    "photo": "image",
    "image": "image",
    "video": "video",
    "audio": "audio",
    "voice_note": "voice_note",
    "document": "document",
    "sticker": "sticker",
}

EXT_MIME_MAP = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".mp4": "video/mp4",
    ".mp3": "audio/mpeg",
    ".ogg": "audio/ogg",
    ".pdf": "application/pdf",
}

ALLOWED_EXT_PER_GATEWAY_TYPE = {
    "image": {".jpg", ".jpeg", ".png", ".webp", ".gif"},
    "video": {".mp4"},
    "audio": {".mp3", ".ogg"},
    "voice_note": {".ogg", ".mp3"},
    "document": {".pdf"},
    "sticker": {".webp", ".png"},
}


class FarrosWAGatewayClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.FARROS_WA_BASE_URL.rstrip("/")
        self.api_key = self.settings.FARROS_WA_API_KEY
        self.session_id = self.settings.FARROS_WA_SESSION_ID
        timeout_sec = float(getattr(self.settings, "FARROS_WA_TIMEOUT", 30))
        self.timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))

    def _get_headers(self, idempotency_key: str | None = None, method: str = "POST") -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        if method.upper() == "POST":
            if not idempotency_key or not IDEMPOTENCY_KEY_REGEX.match(idempotency_key):
                raise GatewayError("Valid Idempotency-Key matching ^[A-Za-z0-9._:-]{8,128}$ is required for all POST requests")
            headers["Idempotency-Key"] = str(idempotency_key)
        return headers

    async def _execute_request(
        self,
        method: str,
        endpoint: str,
        idempotency_key: str | None = None,
        json_data: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        file_info: tuple[Path, str] | None = None,
        max_retries: int = 3,
    ) -> GatewayMessageResponse:
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers(idempotency_key, method=method)

        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if method.upper() == "POST":
                        if file_info:
                            path, mime_type = file_info
                            # Open file fresh on every attempt so stream is at 0 position
                            with open(path, "rb") as f:
                                files = {"file": (path.name, f, mime_type)}
                                response = await client.post(
                                    url, headers=headers, json=json_data, data=data, files=files
                                )
                        else:
                            response = await client.post(
                                url, headers=headers, json=json_data, data=data
                            )
                    else:
                        response = await client.request(
                            method, url, headers=headers, json=json_data, data=data
                        )

                    # Handle 429 immediately — raise typed error, never retry
                    if response.status_code == 429:
                        retry_after = _parse_retry_after(response)
                        raise GatewayRateLimitError(
                            retry_after=retry_after,
                            message=response.text[:200],
                        )

                    # Check for permanent 4xx errors (do not retry 400, 401, 403, 404, 409, 410, 413, 422, etc.)
                    if 400 <= response.status_code < 500:
                        if response.status_code not in (408, 425):
                            raise GatewayResponseError(
                                status_code=response.status_code,
                                message=response.text[:200],
                            )

                    # Check for retryable HTTP errors (408, 425, 5xx)
                    if response.status_code in (408, 425) or response.status_code >= 500:
                        if attempt >= max_retries:
                            raise GatewayResponseError(
                                status_code=response.status_code,
                                message=response.text[:200],
                            )
                        await asyncio.sleep(2 ** attempt)
                        continue

                    # If successful (2xx)
                    response.raise_for_status()
                    try:
                        res_json = response.json()
                        if isinstance(res_json, dict):
                            data_dict = res_json.get("data", res_json)
                            if not isinstance(data_dict, dict):
                                data_dict = res_json

                            msg_id = data_dict.get("id") or data_dict.get("message_id")
                            q_status = data_dict.get("status") or data_dict.get("queue_status")
                            d_status = data_dict.get("delivery_status")

                            # Interpret HTTP 202 without a specific queue_status as 'queued'
                            if response.status_code == 202 and not q_status:
                                q_status = "queued"

                            return GatewayMessageResponse(
                                status="ok",
                                message_id=str(msg_id) if msg_id else None,
                                queue_status=str(q_status) if q_status else None,
                                delivery_status=str(d_status) if d_status else None,
                                http_status=response.status_code,
                                data=data_dict,
                                raw_response=res_json,
                            )
                        return GatewayMessageResponse(status="ok", http_status=response.status_code)
                    except Exception:
                        return GatewayMessageResponse(status="ok", http_status=response.status_code)

            except (GatewayResponseError, GatewayRateLimitError):
                raise
            except httpx.TimeoutException as e:
                if attempt >= max_retries:
                    raise GatewayTimeoutError(f"Request to gateway timed out: {e}") from e
                await asyncio.sleep(2 ** attempt)
            except httpx.RequestError as e:
                if attempt >= max_retries:
                    raise GatewayNetworkError(f"Network error when connecting to gateway: {e}") from e
                await asyncio.sleep(2 ** attempt)

        raise GatewayNetworkError("Max retries exceeded when calling gateway")

    async def send_text(
        self,
        to: str,
        text: str,
        external_reference: str,
        idempotency_key: str | None = None,
    ) -> GatewayMessageResponse:
        payload: dict[str, Any] = {
            "type": "text",
            "to": str(to),
            "text": str(text),
            "external_reference": str(external_reference),
        }
        if self.session_id:
            payload["session_id"] = self.session_id

        return await self._execute_request(
            method="POST",
            endpoint="/api/v1/messages",
            idempotency_key=idempotency_key,
            json_data=payload,
        )

    async def send_media(
        self,
        to: str,
        media_type: str,
        file_path: str,
        caption: str | None = None,
        external_reference: str | None = None,
        idempotency_key: str | None = None,
    ) -> GatewayMessageResponse:
        path = Path(file_path)
        if not path.exists():
            raise GatewayNetworkError(f"File not found: {file_path}")

        if media_type not in GATEWAY_MEDIA_TYPE_MAP:
            raise GatewayError(f"Tipe media '{media_type}' tidak didukung oleh Gateway.")

        gateway_media_type = GATEWAY_MEDIA_TYPE_MAP[media_type]
        suffix = path.suffix.lower()

        if suffix not in EXT_MIME_MAP:
            raise GatewayError(f"Ekstensi file '{suffix}' tidak didukung.")

        allowed_exts = ALLOWED_EXT_PER_GATEWAY_TYPE.get(gateway_media_type, set())
        if allowed_exts and suffix not in allowed_exts:
            raise GatewayError(
                f"Kombinasi tipe media '{gateway_media_type}' dan ekstensi file '{suffix}' tidak cocok."
            )

        mime_type = EXT_MIME_MAP[suffix]

        data: dict[str, Any] = {
            "type": gateway_media_type,
            "to": str(to),
            "filename": path.name,
            "external_reference": str(external_reference) if external_reference else "",
        }
        if caption:
            data["caption"] = str(caption)

        if self.session_id:
            data["session_id"] = self.session_id

        return await self._execute_request(
            method="POST",
            endpoint="/api/v1/messages/upload",
            idempotency_key=idempotency_key,
            data=data,
            file_info=(path, mime_type),
        )

    async def get_message(self, message_id: str) -> GatewayMessageResponse:
        """Fetch message details from gateway."""
        try:
            return await self._execute_request(
                method="GET",
                endpoint=f"/api/v1/messages/{message_id}",
            )
        except GatewayResponseError as e:
            if e.status_code == 404:
                return GatewayMessageResponse(
                    status="not_found",
                    http_status=404,
                    delivery_status=None,
                    data={"error_code": "MESSAGE_NOT_FOUND", "error_message": "Message not found in Gateway (404)"}
                )
            if e.status_code in (401, 403):
                logger.error("Gateway authentication/authorization error. Check API key.")
            raise

    async def get_default_session(self) -> dict[str, Any]:
        try:
            resp = await self._execute_request(
                method="GET",
                endpoint="/api/v1/sessions/default",
            )
            payload = resp.data or {}
            session_data = payload.get("data", payload)

            status = session_data.get("status", "unknown")
            return {
                "status": status,
                "connected": bool(session_data.get("connected")),
                "session_id": session_data.get("id"),
                "name": session_data.get("name")
            }
        except GatewayResponseError as e:
            logger.error(f"Failed to fetch session status: {e}")
            return {"status": "unavailable", "connected": False, "error": str(e)}
        except Exception as e:
            logger.error(f"Network error fetching session status: {e}")
            return {"status": "unavailable", "connected": False}


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Parse Retry-After header, returning seconds as float or None."""
    raw = response.headers.get("Retry-After") or response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None
