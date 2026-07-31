from unittest.mock import MagicMock, patch

import pytest
from app.gateway.client import FarrosWAGatewayClient


@pytest.mark.asyncio
async def test_send_media_omits_caption_if_none():
    client = FarrosWAGatewayClient()

    with patch.object(client, "_execute_request") as mock_execute:
        mock_execute.return_value = MagicMock(status="ok")

        await client.send_media(
            to="628123456789",
            media_type="video",
            file_path="tests/conftest.py", # Using any existing file for path checks
            caption=None,
            external_reference="test-ref"
        )

        mock_execute.assert_called_once()
        call_kwargs = mock_execute.call_args.kwargs
        data = call_kwargs.get("data", {})

        assert "caption" not in data

@pytest.mark.asyncio
async def test_send_media_includes_caption_if_provided():
    client = FarrosWAGatewayClient()

    with patch.object(client, "_execute_request") as mock_execute:
        mock_execute.return_value = MagicMock(status="ok")

        await client.send_media(
            to="628123456789",
            media_type="photo",
            file_path="tests/conftest.py", # Using any existing file for path checks
            caption="Here is a caption",
            external_reference="test-ref"
        )

        mock_execute.assert_called_once()
        call_kwargs = mock_execute.call_args.kwargs
        data = call_kwargs.get("data", {})

        assert "caption" in data
        assert data["caption"] == "Here is a caption"
