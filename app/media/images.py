import logging
import os
from pathlib import Path

from PIL import Image

from app.config import get_settings

logger = logging.getLogger(__name__)


def process_and_optimize_image(source_path: str, target_dir: Path) -> str | None:
    """
    Check image compatibility and optimize/compress until under MAX_MEDIA_MB limit.
    Preserves original aspect ratio. No collages.
    Returns path to optimized image or source_path if already fine.
    """
    settings = get_settings()
    max_bytes = settings.MAX_MEDIA_MB * 1024 * 1024

    if not os.path.exists(source_path):
        return None

    file_size = os.path.getsize(source_path)
    src_path_obj = Path(source_path)

    # Check if original is compatible format (JPG, PNG) and already under size
    if src_path_obj.suffix.lower() in (".jpg", ".jpeg", ".png") and file_size <= max_bytes:
        return source_path

    # Need conversion or compression to high quality JPEG
    target_path = target_dir / f"{src_path_obj.stem}_optimized.jpg"

    try:
        with Image.open(source_path) as img:
            # Convert RGBA/P to RGB for JPEG
            if img.mode in ("RGBA", "LA", "P"):
                rgb_img = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "RGBA":
                    rgb_img.paste(img, mask=img.split()[3])
                else:
                    rgb_img.paste(img.convert("RGB"))
                img_to_save = rgb_img
            else:
                img_to_save = img.convert("RGB")

            # Try step-by-step quality reduction until file fits within max_bytes
            qualities = [95, 85, 75, 65, 50]
            for q in qualities:
                img_to_save.save(target_path, "JPEG", quality=q, optimize=True)
                if os.path.getsize(target_path) <= max_bytes:
                    return str(target_path.resolve())

            # If still over limit, iteratively resize dimensions while preserving aspect ratio
            current_img = img_to_save.copy()
            while os.path.getsize(target_path) > max_bytes and current_img.width > 300 and current_img.height > 300:
                new_width = int(current_img.width * 0.8)
                new_height = int(current_img.height * 0.8)
                current_img = current_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                current_img.save(target_path, "JPEG", quality=65, optimize=True)

            return str(target_path.resolve())

    except Exception as e:
        logger.error(f"Error optimizing image {source_path}: {e}")
        # Return original if at least it exists, otherwise None
        return source_path if os.path.exists(source_path) else None
