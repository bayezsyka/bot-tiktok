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
    GatewayResponseError,
    GatewayTimeoutError,
)
from app.gateway.schemas import GatewayMessageResponse

logger = logging.getLogger(__name__)

IDEMPOTENCY_KEY_REGEX = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")


class FarrosWAGatewayClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.FARROS_WA_BASE_URL.rstrip("/")
        self.api_key = self.settings.FARROS_WA_API_KEY
        self.session_id = self.settings.FARROS_WA_SESSION_ID
        timeout_sec = float(getattr(self.settings, "FARROS_WA_TIMEOUT", 30))
        self.timeout = httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec))

    def _get_headers(self, idempotency_key: str | None = None) -> dict[str, str]:
        if not idempotency_key or not IDEMPOTENCY_KEY_REGEX.match(idempotency_key):
            raise GatewayError("Valid Idempotency-Key matching ^[A-Za-z0-9._:-]{8,128}$ is required for all outbound requests")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Idempotency-Key": str(idempotency_key),
        }
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
        headers = self._get_headers(idempotency_key)

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

                    # Check for permanent 4xx errors (do not retry 400, 401, 403, 404, 409, 410, 413, 422, etc.)
                    if 400 <= response.status_code < 500:
                        if response.status_code not in (408, 425, 429):
                            raise GatewayResponseError(
                                status_code=response.status_code,
                                message=response.text[:200],
                            )

                    # Check for retryable HTTP errors (408, 425, 429, 5xx)
                    if response.status_code in (408, 425, 429) or response.status_code >= 500:
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
                            data_dict = res_json.get("data")
                            msg_id = None
                            if isinstance(data_dict, dict):
                                msg_id = data_dict.get("id") or data_dict.get("message_id")
                            if not msg_id:
                                msg_id = res_json.get("id") or res_json.get("message_id")
                            return GatewayMessageResponse(status="ok", message_id=str(msg_id) if msg_id else None, data=res_json)
                        return GatewayMessageResponse(status="ok")
                    except Exception:
                        return GatewayMessageResponse(status="ok")

            except GatewayResponseError:
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
        caption: str,
        external_reference: str,
        idempotency_key: str | None = None,
    ) -> GatewayMessageResponse:
        path = Path(file_path)
        if not path.exists():
            raise GatewayNetworkError(f"File not found: {file_path}")

        data: dict[str, Any] = {
            "type": str(media_type),
            "to": str(to),
            "filename": path.name,
            "caption": str(caption),
            "external_reference": str(external_reference),
        }
        if self.session_id:
            data["session_id"] = self.session_id

        # Detect mime
        mime_type = "video/mp4" if media_type == "video" else "image/jpeg"
        if path.suffix.lower() == ".png":
            mime_type = "image/png"
        elif path.suffix.lower() == ".webp":
            mime_type = "image/webp"

        return await self._execute_request(
            method="POST",
            endpoint="/api/v1/messages/upload",
            idempotency_key=idempotency_key,
            data=data,
            file_info=(path, mime_type),
        )
