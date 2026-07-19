import asyncio
import logging
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.gateway.exceptions import GatewayNetworkError, GatewayResponseError, GatewayTimeoutError
from app.gateway.schemas import GatewayMessageResponse

logger = logging.getLogger(__name__)


class FarrosWAGatewayClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.FARROS_WA_BASE_URL.rstrip("/")
        self.api_key = self.settings.FARROS_WA_API_KEY
        self.session_id = self.settings.FARROS_WA_SESSION_ID
        self.timeout = httpx.Timeout(30.0, connect=10.0)

    def _get_headers(self, idempotency_key: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        if idempotency_key:
            headers["Idempotency-Key"] = str(idempotency_key)
        return headers

    async def _execute_request(
        self,
        method: str,
        endpoint: str,
        idempotency_key: str | None = None,
        json_data: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
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
                        response = await client.post(
                            url, headers=headers, json=json_data, data=data, files=files
                        )
                    else:
                        response = await client.request(
                            method, url, headers=headers, json=json_data, data=data
                        )

                    # Check for 4xx permanent errors
                    if 400 <= response.status_code < 500:
                        raise GatewayResponseError(
                            status_code=response.status_code,
                            message=response.text[:200],
                        )

                    # Check for 5xx transient server errors
                    if response.status_code >= 500:
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
                            msg_id = res_json.get("message_id") or res_json.get("data", {}).get("id") or res_json.get("id")
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

        with open(path, "rb") as f:
            files = {"file": (path.name, f, mime_type)}
            return await self._execute_request(
                method="POST",
                endpoint="/api/v1/messages/upload",
                idempotency_key=idempotency_key,
                data=data,
                files=files,
            )
