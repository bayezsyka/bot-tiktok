import asyncio
import logging
import math
import os
import shutil
from pathlib import Path
from typing import Sequence

from app.config import get_settings

logger = logging.getLogger(__name__)


async def render_slideshow_video(
    image_paths: Sequence[str | Path],
    audio_path: str | Path | None,
    output_path: str | Path,
    duration_seconds: int = 0,
) -> bool:
    """
    Render images and audio into an MP4 video preserving image quality and resolution.
    - If 1 image: loop image for audio duration.
    - If multiple images: allocate proportional duration per image, then stitch with audio.
    """
    settings = get_settings()
    ffmpeg_bin = getattr(settings, "FFMPEG_BINARY", "ffmpeg")

    img_list = [Path(p) for p in image_paths if os.path.exists(p)]
    if not img_list:
        logger.error("No valid images found to render slideshow.")
        return False

    out_p = Path(output_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    # 1. Single Image Slideshow
    if len(img_list) == 1:
        img = img_list[0]
        if audio_path and os.path.exists(audio_path):
            cmd = [
                ffmpeg_bin,
                "-y",
                "-loop", "1",
                "-framerate", "2",
                "-i", str(img),
                "-i", str(audio_path),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-tune", "stillimage",
                "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
                "-c:a", "aac",
                "-b:a", "128k",
                "-pix_fmt", "yuv420p",
                "-shortest",
                "-movflags", "+faststart",
                str(out_p),
            ]
        else:
            # No audio, loop 5 seconds default
            cmd = [
                ffmpeg_bin,
                "-y",
                "-loop", "1",
                "-framerate", "2",
                "-t", "5",
                "-i", str(img),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-tune", "stillimage",
                "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                str(out_p),
            ]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300.0)
        if proc.returncode == 0 and out_p.exists() and out_p.stat().st_size > 0:
            return True
        logger.error(f"Single image render failed: {stderr.decode(errors='replace')[:300]}")
        return False

    # 2. Multi-Image Slideshow
    # Calculate duration per slide
    total_audio_duration = float(duration_seconds) if duration_seconds > 0 else 0.0
    if not total_audio_duration and audio_path and os.path.exists(audio_path):
        from app.media.ffmpeg import probe_video
        probe = await probe_video(str(audio_path))
        try:
            total_audio_duration = float(probe.get("format", {}).get("duration", 0))
        except Exception:
            total_audio_duration = 0.0

    if total_audio_duration <= 0:
        slide_duration = 3.0
    else:
        slide_duration = max(2.5, total_audio_duration / len(img_list))

    # Create concat demuxer file
    concat_txt = out_p.parent / "concat_slides.txt"
    with open(concat_txt, "w") as f:
        for img in img_list:
            f.write(f"file '{img.resolve()}'\n")
            f.write(f"duration {slide_duration:.3f}\n")
        # Concat demuxer requirement: repeat last file without duration
        f.write(f"file '{img_list[-1].resolve()}'\n")

    if audio_path and os.path.exists(audio_path):
        cmd = [
            ffmpeg_bin,
            "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_txt),
            "-i", str(audio_path),
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
            "-c:a", "aac",
            "-b:a", "128k",
            "-pix_fmt", "yuv420p",
            "-shortest",
            "-movflags", "+faststart",
            str(out_p),
        ]
    else:
        cmd = [
            ffmpeg_bin,
            "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_txt),
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            str(out_p),
        ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300.0)
        if concat_txt.exists():
            concat_txt.unlink(missing_ok=True)

        if proc.returncode == 0 and out_p.exists() and out_p.stat().st_size > 0:
            return True
        logger.error(f"Multi-image slideshow render failed: {stderr.decode(errors='replace')[:300]}")
        return False
    except Exception as e:
        if concat_txt.exists():
            concat_txt.unlink(missing_ok=True)
        logger.error(f"Slideshow render exception: {e}")
        return False
