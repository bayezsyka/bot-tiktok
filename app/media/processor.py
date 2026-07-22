import logging
import os
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database.models import DownloadJob
from app.media.ffmpeg import (
    calculate_target_bitrate_kbps,
    compress_video,
    probe_video,
    remux_to_mp4,
)
from app.media.images import process_and_optimize_image

logger = logging.getLogger(__name__)


class MediaProcessor:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.settings = get_settings()

    async def process_job_media(self, job: DownloadJob, job_dir: Path) -> None:
        """
        Inspect and process all media items for a job.
        Ensures each item meets formatting compatibility and fits within MAX_MEDIA_MB limit.
        """
        max_bytes = self.settings.MAX_MEDIA_MB * 1024 * 1024
        total_final_size = 0

        for item in job.items:
            if item.status == "sent" or item.gateway_message_id:
                if item.local_filename and os.path.exists(item.local_filename):
                    total_final_size += os.path.getsize(item.local_filename)
                continue

            if not item.local_filename or not os.path.exists(item.local_filename):
                item.status = "failed"
                item.error_message = "File fisik hasil download tidak ditemukan"
                continue

            item.status = "processing"
            await self.session.flush()

            source_path = item.local_filename
            file_size = os.path.getsize(source_path)

            if item.media_type == "video":
                processed_path = await self._process_video_item(source_path, file_size, max_bytes, job_dir)
            else:
                processed_path = process_and_optimize_image(source_path, job_dir)

            if processed_path and os.path.exists(processed_path):
                final_size = os.path.getsize(processed_path)
                if final_size > max_bytes:
                    item.status = "failed"
                    item.error_message = f"Ukuran media akhir ({final_size // (1024*1024)}MB) melebihi batas {self.settings.MAX_MEDIA_MB}MB"
                else:
                    item.local_filename = processed_path
                    item.final_size_bytes = final_size
                    item.status = "pending"  # ready for sending
                    total_final_size += final_size
            else:
                item.status = "failed"
                item.error_message = "Gagal memproses media agar kompatibel dan di bawah batas ukuran"

        job.final_size_bytes = total_final_size
        await self.session.flush()

    async def _process_video_item(
        self, source_path: str, file_size: int, max_bytes: int, job_dir: Path
    ) -> str | None:
        # If already below size and is mp4, check if container is clean or remux
        src_path_obj = Path(source_path)

        if file_size <= max_bytes and src_path_obj.suffix.lower() == ".mp4":
            # Optional fast remux to ensure clean faststart container
            remux_path = str(job_dir / f"{src_path_obj.stem}_remux.mp4")
            if await remux_to_mp4(source_path, remux_path):
                return remux_path
            return source_path

        # If over limit or non-mp4, probe duration
        probe_data = await probe_video(source_path)
        format_info = probe_data.get("format", {})
        duration = float(format_info.get("duration") or 0)

        if duration <= 0:
            # Fallback estimation if ffprobe duration fails
            duration = 60.0

        # Pass 1: compress with target bitrate up to 1080p
        target_bitrate = calculate_target_bitrate_kbps(duration, max_bytes)
        compressed_path_p1 = str(job_dir / f"{src_path_obj.stem}_comp1080.mp4")

        success = await compress_video(
            source_path=source_path,
            target_path=compressed_path_p1,
            target_bitrate_kbps=target_bitrate,
            max_resolution=1080,
        )

        if success and os.path.exists(compressed_path_p1) and os.path.getsize(compressed_path_p1) <= max_bytes:
            return compressed_path_p1

        # Pass 2: if still over limit or pass 1 failed, compress down to max 720p with slightly tighter bitrate
        compressed_path_p2 = str(job_dir / f"{src_path_obj.stem}_comp720.mp4")
        tighter_bitrate = max(int(target_bitrate * 0.8), 150)

        success_p2 = await compress_video(
            source_path=source_path,
            target_path=compressed_path_p2,
            target_bitrate_kbps=tighter_bitrate,
            max_resolution=720,
        )

        if success_p2 and os.path.exists(compressed_path_p2) and os.path.getsize(compressed_path_p2) <= max_bytes:
            return compressed_path_p2

        # Return whatever is smaller between p1 and original if p2 failed
        if os.path.exists(compressed_path_p1) and os.path.getsize(compressed_path_p1) <= max_bytes:
            return compressed_path_p1

        return source_path
