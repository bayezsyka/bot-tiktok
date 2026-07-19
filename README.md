# Farros TikTok Bot

A production-ready WhatsApp & TikTok Gateway application built with FastAPI, SQLAlchemy 2, SQLite, and yt-dlp. Designed to receive webhook notifications from **Farros WA Gateway**, download TikTok videos (without watermark) and high-resolution photo slideshows, optimize media size/format for WhatsApp limits, and send them back to the user seamlessly.

---

## ⚡ Key Features

- **Webhook Verification**: Constant-time HMAC-SHA256 signature checking (`X-FWAG-Signature`) with timestamp tolerance (`X-FWAG-Timestamp`) to prevent replay attacks and spoofing.
- **Defensive Message Parsing**: Extracts sender numbers, timestamps, and message payloads safely, rejecting broadcast/group messages (`is_group`) and status updates.
- **SSRF & URL Hardening**: Robust regex-based TikTok URL extraction and canonical resolution with strict domain and scheme checks to prevent Server-Side Request Forgery.
- **Single-Job Queue Worker**: Persistent SQLite-backed job queue guaranteeing **ONE job processed at a time** to prevent server overload and memory spikes.
- **Auto Recovery on Restart**: Automatically recovers incomplete jobs interrupted by system restarts and returns them to the queue if retries remain.
- **Smart Media Optimization**:
  - **Video (`yt-dlp` + `FFmpeg`)**: Downloads highest quality non-watermark video. If it exceeds gateway size limits, automatically compresses H.264/AAC using 2-pass target bitrate down to 1080p/720p without distorting aspect ratios.
  - **Photos (`PIL` + `httpx`)**: Extracts full slideshow sequence from TikTok rehydration scripts, downloads raw images with magic byte signature verification, and compresses/resizes proportionally if exceeding size limits.
- **Security Hardened**:
  - CSRF tokens (`X-CSRF-Token` & form validation) across all state-changing endpoints.
  - In-memory sliding window rate limiter against spam across webhook and admin logins.
  - Argon2 (`argon2-cffi`) password hashing with automatic session rotation (`SessionMiddleware`) on login.
- **Admin Dashboard UI**: Responsive Jinja2 + HTMX + Vanilla CSS dark/light comfortable UI without external frontend build dependencies. Includes real-time statistics, whitelist management, detailed job timeline, and retry buttons.
- **Command Line Tool (`cli.py`)**: Built-in CLI management utility for database init, admin management, whitelist manipulation, and comprehensive system health diagnostics.

---

## 🏗 Architecture Overview

```
 [ WhatsApp User ] ---> (Farros WA Gateway)
                               |
                               | POST /webhooks/farros-wa (HMAC-SHA256)
                               v
                     [ FastAPI Router ] ---> [ SQLite Queue ]
                                                   |
                     +-----------------------------+
                     v
             [ Queue Worker ] (Background Task - ONE at a time)
                     |
         +-----------+-----------+
         |                       |
         v                       v
 [ yt-dlp Video ]       [ Photo Slideshow ]
         |                       |
         +-----------+-----------+
                     v
             [ Media Processor ] (FFmpeg / PIL Optimization)
                     |
                     v
      [ Farros WA Gateway Client ] (HTTPX with Idempotency & Retries)
                     |
                     v
              [ WhatsApp User ]
```

See `docs/ARCHITECTURE.md` for detailed flow diagrams, database schemas, and state transitions.

---

## 🚀 Quick Start & Installation

### 1. Requirements
- **OS**: Linux / macOS
- **Python**: 3.12+
- **System Binaries**: `yt-dlp`, `ffmpeg`, `ffprobe` (must be installed and available in `$PATH` or configured in `.env`).

### 2. Clone & Virtual Environment Setup
```bash
git clone https://github.com/farros/bot-tiktok.git
cd bot-tiktok

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Configuration (`.env`)
Copy the template and adjust settings:
```bash
cp .env.example .env
```
Key configuration variables:
```ini
APP_ENV="production"
APP_PORT=3200
DATABASE_PATH="storage/database.sqlite"
TEMP_DIR="storage/temp"

# Gateway credentials
FARROS_WA_BASE_URL="https://gateway.farros.id"
FARROS_WA_API_KEY="your-api-key"
FARROS_WA_WEBHOOK_SECRET="your-webhook-secret"

# Limits & security
MAX_MEDIA_MB=16
MAX_SOURCE_DOWNLOAD_MB=100
MAX_VIDEO_DURATION_SECONDS=600
RATE_LIMIT_REQUESTS=10
RATE_LIMIT_WINDOW_MINUTES=5
```

### 4. Initialize Database & Create Admin
Use the included CLI tool:
```bash
# Initialize SQLite WAL database and run migrations
python cli.py init-db

# Create your first admin account
python cli.py create-admin --username admin --password "StrongPasswordHere!" --email admin@farros.id

# Add an initial WhatsApp number to the whitelist (format 628xxx)
python cli.py add-number --phone 628123456789 --name "Farros Main"
```

### 5. Run the Application
For local development:
```bash
make dev
# or: .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 3200 --reload
```

---

## 🛠 CLI Management Commands (`cli.py`)

Run `python cli.py --help` for full usage details:

| Command | Description | Example |
| :--- | :--- | :--- |
| `init-db` | Initialize database schema and migrations | `python cli.py init-db` |
| `create-admin` | Create or update admin account | `python cli.py create-admin --username admin --password pass` |
| `reset-password`| Reset password for existing admin | `python cli.py reset-password --username admin --password pass` |
| `list-numbers` | Display all numbers in the whitelist | `python cli.py list-numbers` |
| `add-number` | Add a WhatsApp number to whitelist | `python cli.py add-number --phone 628123456789 --name "Dev"` |
| `remove-number` | Remove a number from the whitelist | `python cli.py remove-number --phone 628123456789` |
| `retry-job` | Requeue a failed download/send job | `python cli.py retry-job --id "job-uuid"` |
| `prune-temp` | Clean up expired temporary items | `python cli.py prune-temp --ttl-minutes 60` |
| `check-health` | Run diagnostics (DB, disk space, yt-dlp) | `python cli.py check-health` |

---

## 🧪 Testing & Quality Assurance

The project includes an automated offline test suite covering signature verification, URL extraction, rate limiting, photo post extraction, single-job queue worker lifecycle, and CLI commands without making external network calls.

```bash
# Run full pytest suite with verbose output
make test
# or: .venv/bin/pytest -v

# Run linter
make lint

# Run type checker
make typecheck
```

---

## 📦 Production Deployment (systemd)

1. Copy repository to `/var/www/farros-tiktok-bot`:
```bash
sudo mkdir -p /var/www/farros-tiktok-bot
sudo cp -r . /var/www/farros-tiktok-bot/
sudo chown -R www-data:www-data /var/www/farros-tiktok-bot
```

2. Install and enable systemd service:
```bash
sudo cp deploy/farros-tiktok-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable farros-tiktok-bot
sudo systemctl start farros-tiktok-bot
sudo systemctl status farros-tiktok-bot
```

### Security Hardening in Service Unit
The systemd service unit enforces:
- `ProtectSystem=full`: Mounts `/usr`, `/boot`, and `/etc` read-only for the service.
- `PrivateTmp=true`: Isolates temporary directory inside namespace.
- `NoNewPrivileges=true`: Prevents privilege escalation.

---

## 📜 License & Ownership
Copyright &copy; 2026 Farros Sangkolo. All Rights Reserved. Production-ready internal solution for automated media handling.
