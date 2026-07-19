import asyncio
import json
import logging
import os
from typing import Any

from app.config import get_settings

logger = logging.getLogger(__name__)


async def probe_video(file_path: str) -> dict[str, Any]:
    """Run ffprobe via subprocess argument array to extract video duration, size, and codecs."""
    settings = get_settings()
    args = [
        settings.FFPROBE_BINARY,
        "-v",
        "error",
        "-show_entries",
        "format=duration,size:stream=codec_name,codec_type,width,height",
        "-of",
        "json",
        file_path,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
        if proc.returncode != 0:
            logger.warning(f"ffprobe returned error code {proc.returncode}: {stderr.decode(errors='replace')}")
            return {}
        res: dict[str, Any] = json.loads(stdout.decode("utf-8", errors="replace"))
        return res
    except Exception as e:
        logger.error(f"Error running ffprobe on {file_path}: {e}")
        return {}


def calculate_target_bitrate_kbps(duration_seconds: float, target_size_bytes: int, audio_bitrate_kbps: int = 96) -> int:
    """Calculate target video bitrate in kbps with a safety margin (~88%) so final file is below limit."""
    if duration_seconds <= 0:
        return 1000  # fallback

    safe_target_bytes = target_size_bytes * 0.88
    total_bitrate_kbps = (safe_target_bytes * 8) / (duration_seconds * 1000)
    video_bitrate_kbps = int(total_bitrate_kbps - audio_bitrate_kbps)

    # Clamp minimum bitrate to 150 kbps
    return max(video_bitrate_kbps, 150)


async def remux_to_mp4(source_path: str, target_path: str) -> bool:
    """Remux video container to MP4 without re-encoding."""
    settings = get_settings()
    args = [
        settings.FFMPEG_BINARY,
        "-y",
        "-i",
        source_path,
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        target_path,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120.0)
        if proc.returncode == 0 and os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            return True
        logger.warning(f"Remux failed: {stderr.decode(errors='replace')[:200]}")
        return False
    except Exception as e:
        logger.error(f"Remux exception: {e}")
        return False


async def compress_video(
    source_path: str, target_path: str, target_bitrate_kbps: int, max_resolution: int = 1080
) -> bool:
    """Encode video to H.264 (veryfast) + AAC with target bitrate and resolution constraint."""
    settings = get_settings()

    # Scale maintaining aspect ratio, ensuring even dimensions required by H.264
    vf_scale = f"scale='min({max_resolution},iw)':'min({max_resolution},ih)':force_original_aspect_ratio=decrease,trunc(iw/2)*2:trunc(ih/2)*2"

    args = [
        settings.FFMPEG_BINARY,
        "-y",
        "-i",
        source_path,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-b:v",
        f"{target_bitrate_kbps}k",
        "-maxrate",
        f"{int(target_bitrate_kbps * 1.2)}k",
        "-bufsize",
        f"{int(target_bitrate_kbps * 2)}k",
        "-vf",
        vf_scale,
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        target_path,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=float(settings.JOB_TIMEOUT_SECONDS))
        if proc.returncode == 0 and os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            return True
        logger.warning(f"FFmpeg compress failed: {stderr.decode(errors='replace')[:200]}")
        return False
    except Exception as e:
        logger.error(f"FFmpeg compression exception: {e}")
        return False
