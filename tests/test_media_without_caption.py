from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from app.gateway.client import FarrosWAGatewayClient


@pytest.mark.asyncio
async def test_send_media_omits_caption_if_none(tmp_path: Path) -> None:
    client = FarrosWAGatewayClient()
    vid_file = tmp_path / "video.mp4"
    vid_file.write_bytes(b"\x00\x00\x00\x1cftypisom")

    with patch.object(client, "_execute_request") as mock_execute:
        mock_execute.return_value = MagicMock(status="ok")

        await client.send_media(
            to="628123456789",
            media_type="video",
            file_path=str(vid_file),
            caption=None,
            external_reference="test-ref",
            idempotency_key="test-idemp-key-01",
        )

        mock_execute.assert_called_once()
        call_kwargs = mock_execute.call_args.kwargs
        data = call_kwargs.get("data", {})

        assert "caption" not in data


@pytest.mark.asyncio
async def test_send_media_includes_caption_if_provided(tmp_path: Path) -> None:
    client = FarrosWAGatewayClient()
    img_file = tmp_path / "photo.jpg"
    img_file.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")

    with patch.object(client, "_execute_request") as mock_execute:
        mock_execute.return_value = MagicMock(status="ok")

        await client.send_media(
            to="628123456789",
            media_type="photo",
            file_path=str(img_file),
            caption="Here is a caption",
            external_reference="test-ref",
            idempotency_key="test-idemp-key-02",
        )

        mock_execute.assert_called_once()
        call_kwargs = mock_execute.call_args.kwargs
        data = call_kwargs.get("data", {})

        assert "caption" in data
        assert data["caption"] == "Here is a caption"
