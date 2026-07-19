import logging
import shutil
import time
from pathlib import Path

from app.config import get_settings

logger = logging.getLogger(__name__)


def get_resolved_temp_dir() -> Path:
    settings = get_settings()
    return Path(settings.TEMP_DIR).resolve()


def is_safe_temp_path(target_path: Path) -> bool:
    """Verify that target_path is strictly inside TEMP_DIR (path traversal protection)."""
    try:
        resolved_target = target_path.resolve()
        resolved_temp = get_resolved_temp_dir()
        return resolved_target.is_relative_to(resolved_temp) and resolved_target != resolved_temp
    except Exception:
        return False


def create_job_temp_dir(job_id: str) -> Path:
    """Create and return resolved path to unique temporary folder for a job."""
    temp_dir = get_resolved_temp_dir()
    job_dir = (temp_dir / str(job_id)).resolve()

    if not is_safe_temp_path(job_dir):
        raise ValueError(f"Unsafe path detected for job directory: {job_dir}")

    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def remove_job_temp_dir(job_id: str) -> bool:
    """Safely remove the temporary folder for a job if inside TEMP_DIR."""
    temp_dir = get_resolved_temp_dir()
    job_dir = (temp_dir / str(job_id)).resolve()

    if not is_safe_temp_path(job_dir):
        logger.warning(f"Attempted to remove path outside TEMP_DIR: {job_dir}")
        return False

    if job_dir.exists() and job_dir.is_dir():
        try:
            shutil.rmtree(job_dir, ignore_errors=True)
            return True
        except Exception as e:
            logger.error(f"Failed to remove job directory {job_dir}: {e}")
            return False
    return True


def cleanup_expired_temp_files(ttl_minutes: int) -> int:
    """
    Periodically clean up temporary files/folders older than TTL minutes.
    Only deletes inside TEMP_DIR.
    Returns number of folders removed.
    """
    temp_dir = get_resolved_temp_dir()
    if not temp_dir.exists() or not temp_dir.is_dir():
        return 0

    now = time.time()
    ttl_seconds = ttl_minutes * 60
    removed_count = 0

    for item in temp_dir.iterdir():
        if item.name.startswith(".") or item.name == ".gitkeep":
            continue

        try:
            if not is_safe_temp_path(item.resolve()):
                continue

            stat = item.stat()
            age = now - max(stat.st_mtime, stat.st_ctime)
            if age > ttl_seconds:
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
                removed_count += 1
        except Exception as e:
            logger.warning(f"Error during temp cleanup on {item}: {e}")

    if removed_count > 0:
        logger.info(f"Cleaned up {removed_count} expired temporary items.")
    return removed_count


def check_disk_space() -> int:
    """Check free disk space in bytes on TEMP_DIR volume. Returns bytes free."""
    temp_dir = get_resolved_temp_dir()
    try:
        temp_dir.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(str(temp_dir))
        return usage.free
    except Exception as e:
        logger.error(f"Failed to check disk space: {e}")
        return 0


def is_disk_space_sufficient(min_free_bytes: int = 1024 * 1024 * 1024) -> bool:
    """Check if free disk space is >= min_free_bytes (default 1 GB)."""
    free_bytes = check_disk_space()
    return free_bytes >= min_free_bytes
