# Farros Media Bot

A production-ready WhatsApp Media Downloader Gateway built with FastAPI, SQLAlchemy 2, SQLite, and yt-dlp. Designed to receive webhook notifications from **Farros WA Gateway**, download TikTok videos (without watermark), TikTok photo slideshows, and Instagram Reels videos, optimize media size/format for WhatsApp limits, and send them back to the user seamlessly.

---

## ⚡ Key Features

- **Multi-Platform Download Support**:
  - **TikTok**: Non-watermarked video and high-resolution photo slideshow posts.
  - **Instagram Reels**: Public Instagram Reels videos (`/reel/SHORTCODE/` and `/reels/SHORTCODE/`).
- **Complete LID & Number Management via Admin UI & CLI**:
  - Whitelist phone numbers (format `628...`) and pair WhatsApp LID (`@lid`) digits directly in the dashboard (`/admin/allowed-numbers`).
  - Track incoming unmapped `@lid` senders automatically in `unmapped_lids` table (`/admin/unmapped-lids`), allowing 1-click pairing to existing numbers or new number creation.
  - Import fallback mappings from environment variable (`FARROS_WA_LID_MAP`) with conflict detection.
- **Webhook Verification & Security**:
  - Constant-time HMAC-SHA256 signature checking (`X-FWAG-Signature`) with timestamp tolerance (`X-FWAG-Timestamp`) to prevent replay attacks and spoofing.
  - Defensive message parsing: extracts senders, LID numbers, and text previews, ignoring broadcast/group messages (`is_group`) and status updates.
  - SSRF & URL Hardening: Strict domain allowlists, local path validation, scheme checks, and canonical URL resolution.
- **Single-Job Queue Worker**: Persistent SQLite-backed job queue guaranteeing **ONE job processed at a time** across all platforms to prevent server overload and memory spikes.
- **Smart Media Optimization**:
  - **Video (`yt-dlp` + `FFmpeg`)**: Downloads highest quality non-watermark video. Automatically compresses H.264/AAC using 2-pass target bitrate down to 1080p/720p if exceeding gateway size limits.
  - **Photos (`PIL` + `httpx`)**: Extracts full slideshow sequence from TikTok, downloads raw images with magic byte verification, and compresses proportionally.
- **Security Hardened**:
  - CSRF tokens (`X-CSRF-Token` & form validation) across all state-changing endpoints.
  - Rate limiting against spam per WhatsApp number across TikTok and Instagram requests.
  - Argon2 (`argon2-cffi`) password hashing with automatic session rotation on login.
- **Admin Dashboard UI**: Responsive Jinja2 + HTMX UI with real-time statistics, platform breakdown (TikTok vs Instagram), LID management, job timeline, and retry buttons.
- **CLI Tool (`cli.py`)**: Built-in CLI management utility for database init, admin management, whitelist manipulation, LID pairing, unmapped LID listing, and health diagnostics.

---

## 🔗 Supported URL Formats

### TikTok
- `https://www.tiktok.com/@user/video/1234567890`
- `https://vt.tiktok.com/ABCDEF/`
- `https://vm.tiktok.com/ABCDEF/`
- Photo slideshow posts (`https://www.tiktok.com/@user/photo/1234567890`)

### Instagram Reels
- `https://www.instagram.com/reel/SHORTCODE/`
- `https://instagram.com/reel/SHORTCODE/`
- `https://www.instagram.com/reels/SHORTCODE/`
- Supports query string parameters (`?igsh=...`, `?utm_source=...`)

> [!NOTE]
> Only **public Instagram Reels videos** are supported. Instagram Stories, DMs, profiles, photo posts, carousels, and private/login-required content are strictly rejected with user-friendly error messages.

---

## 🛠 CLI Management Commands (`cli.py`)

Run `python cli.py --help` for full usage details:

| Command | Description | Example |
| :--- | :--- | :--- |
| `init-db` | Initialize database schema and migrations | `python cli.py init-db` |
| `create-admin` | Create or update admin account | `python cli.py create-admin --username admin --password pass` |
| `reset-password`| Reset password for existing admin | `python cli.py reset-password --username admin --password pass` |
| `list-numbers` | Display all numbers and LID mappings in whitelist | `python cli.py list-numbers` |
| `add-number` | Add a WhatsApp number and optional LID to whitelist | `python cli.py add-number --phone 628123456789 --name "Dev" --lid 12345678901234` |
| `update-number` | Update an existing number/LID entry | `python cli.py update-number --phone 628123456789 --name "New Name" --lid 123456` |
| `assign-lid` | Assign an LID to a registered WhatsApp number | `python cli.py assign-lid --phone 628123456789 --lid 12345678901234` |
| `remove-lid` | Remove LID mapping from a phone number | `python cli.py remove-lid --phone 628123456789` |
| `toggle-number` | Toggle active/inactive status of a number | `python cli.py toggle-number --phone 628123456789` |
| `remove-number` | Remove a number from the whitelist | `python cli.py remove-number --phone 628123456789` |
| `list-unmapped-lids` | List all unmapped LID records | `python cli.py list-unmapped-lids` |
| `retry-job` | Requeue a failed download/send job | `python cli.py retry-job --id "job-uuid"` |
| `prune-temp` | Clean up expired temporary items | `python cli.py prune-temp --ttl-minutes 60` |
| `check-health` | Run diagnostics (DB, disk space, binaries) | `python cli.py check-health` |

---

## 🧪 Testing & Quality Check

Run full test suite and code quality validation:
```bash
# Compile all source files
.venv/bin/python -m compileall app tests cli.py

# Run pytest test suite
.venv/bin/pytest -q

# Run ruff check
.venv/bin/ruff check .

# Run mypy static type analysis
.venv/bin/mypy app tests

# Check git diff formatting
git diff --check
```

---

## 📜 License & Ownership
Copyright &copy; 2026 Farros Sangkolo. All Rights Reserved. Production-ready internal solution for automated media handling.
