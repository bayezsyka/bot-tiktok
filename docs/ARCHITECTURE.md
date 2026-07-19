# Farros TikTok Bot - Technical Architecture Document

## 1. System Components & Separation of Concerns

```
                                  +-----------------------+
                                  |   Farros WA Gateway   |
                                  +-----------+-----------+
                                              |
                                     Webhook  | (POST /webhooks/farros-wa)
                                     Signed   | HMAC-SHA256
                                              v
+-----------------------------------------------------------------------------------+
| FastAPI Application Layer (`app/`)                                                |
|                                                                                   |
|  +------------------------+      +---------------------+      +----------------+  |
|  |   app/webhooks/        | ---> |   app/security/     | ---> | app/database/  |  |
|  |   Signature, Parser    |      |   URL, RateLimiter  |      | Repositories   |  |
|  +------------------------+      +---------------------+      +-------+--------+  |
|                                                                       |           |
+-----------------------------------------------------------------------|-----------+
                                                                        |
                                                                        v
+-----------------------------------------------------------------------------------+
| Background Worker & Queue Layer (`app/queue/`)                        |           |
|                                                                       |           |
|  +------------------------+      +---------------------+              |           |
|  |   app/queue/worker     | <--- |  SQLite WAL Queue   | <------------+           |
|  |   (Single Job Loop)    |      |  (DownloadJob Table)|                          |
|  +-----------+------------+      +---------------------+                          |
|              |                                                                    |
+--------------|--------------------------------------------------------------------+
               |
               +-----------------------------+
               |                             |
               v                             v
+------------------------------+   +------------------------------+
| app/downloader/              |   | app/media/                   |
| - YtDlpProvider (subprocess) |   | - FFmpeg remux & compress    |
| - TikTokPhotoProvider        |   | - PIL Image optimization     |
| - DownloaderService          |   | - Safe temp path manager     |
+--------------+---------------+   +---------------+--------------+
               |                                   |
               +-----------------+-----------------+
                                 |
                                 v
                 +-------------------------------+
                 | app/gateway/                  |
                 | FarrosWAGatewayClient (HTTPX) |
                 +---------------+---------------+
                                 |
                                 v
                   +---------------------------+
                   |     Farros WA Gateway     |
                   +---------------------------+
```

---

## 2. Queue Job State Machine

Each `DownloadJob` undergoes strict status transitions:

```
                  +-----------+
                  |  queued   | <---------------+
                  +-----+-----+                 |
                        |                       |
                        v                       |
                  +-----------+                 |
                  | extracting|                 |
                  +-----+-----+                 |
                        |                       |
                        v                       |
                  +-----------+                 |
                  |downloading|                 |
                  +-----+-----+                 |
                        |                       |
                        v                       |
                  +-----------+                 |
                  |processing |                 |
                  +-----+-----+                 |
                        |                       |
                        v                       |
                  +-----------+                 |
                  |  sending  |                 |
                  +-----+-----+                 |
                        |                       |
         +--------------+--------------+        |
         |                             |        |
         v                             v        |
  +-------------+               +-------------+ |
  |  completed  |               |   failed    | |
  +-------------+               +------+------+ |
                                       |        |
                                       +--------+ (via retry_failed_job or Worker retry)
```

### State Descriptions:
1. `queued`: Job created in database by `app/webhooks/router.py` after signature verification and whitelist checking.
2. `extracting`: `QueueWorker` acquires job inside a database transaction, sets start time, and determines if URL is video (`YtDlpProvider`) or photo slideshow (`TikTokPhotoProvider`).
3. `downloading`: Physical media items (`video_source.mp4` or `photo_001.jpg`, etc.) are streamed and downloaded to the job's unique temporary directory `storage/temp/<uuid>/`.
4. `processing`: `MediaProcessor` inspects physical files, verifies compatibility, and compresses media below `MAX_MEDIA_MB` limit using FFmpeg or PIL.
5. `sending`: `FarrosWAGatewayClient` sends media files to `Farros WA Gateway` using unique `Idempotency-Key` headers.
6. `completed`: All media items marked `sent`. Temporary directory removed.
7. `failed`: Permanent error encountered or max retries exhausted. Temporary directory removed.

---

## 3. Database Entity Relationship Diagram (ERD)

```
+---------------------+          +-----------------------+          +--------------------+
|    AllowedNumber    |          |      DownloadJob      |          |    DownloadItem    |
+---------------------+          +-----------------------+          +--------------------+
| id (PK)             |          | id (UUID PK)          |          | id (PK)            |
| phone_number (UQ)   |          | inbound_message_id    | <+-------| job_id (FK)        |
| name                |          | webhook_event_id      |  |       | position           |
| notes               |          | sender_number         |  |       | media_type         |
| is_active           |          | original_url          |  |       | source_url         |
| total_jobs          |          | canonical_url         |  |       | local_filename     |
| last_used_at        |          | content_type          |  |       | source_size_bytes  |
| created_at          |          | status                |  |       | final_size_bytes   |
| updated_at          |          | attempt_count         |  |       | status             |
+---------------------+          | media_count           |  |       | gateway_message_id |
                                 | sent_count            |  |       | error_message      |
+---------------------+          | failed_count          |  |       +--------------------+
|     WebhookEvent    |          | source_size_bytes     |  |
+---------------------+          | final_size_bytes      |  |       +--------------------+
| id (PK)             |          | duration_seconds      |  |       |       Admin        |
| event_id (UQ Index) |          | error_code            |  |       +--------------------+
| event_type          |          | error_message         |  |       | id (PK)            |
| payload_hash        |          | queued_at             |  |       | username (UQ)      |
| created_at          |          | started_at            |  |       | password_hash      |
+---------------------+          | completed_at          |  |       | email              |
                                 | updated_at            |  |       | is_active          |
                                 +-----------------------+  |       | last_login_at      |
                                                            |       | created_at         |
                                                            +       +--------------------+
```

---

## 4. Security Mitigations & Hardening

1. **SSRF Protection (`app/security/urls.py`)**:
   - `resolve_canonical_tiktok_url` limits redirect chains to max 5 jumps.
   - Enforces `https://` or `http://` schemes only.
   - Validates that target hostname ends in `.tiktok.com` or `.tiktokcdn.com` or exact `tiktok.com`.
   - Rejects loopback addresses (`127.0.0.1`, `localhost`), internal private subnets (`10.x`, `192.168.x`, `172.16.x`), and file (`file://`) schemes.
2. **Command Injection Prevention (`app/downloader/yt_dlp_provider.py`, `app/media/ffmpeg.py`)**:
   - All `subprocess` invocations use exact argument list arrays (`asyncio.create_subprocess_exec(*args)`).
   - `shell=True` is strictly forbidden across the entire codebase.
3. **Path Traversal Protection (`app/media/cleanup.py`)**:
   - `is_safe_temp_path(target_path)` resolves path via `.resolve()` and confirms it is strictly inside `TEMP_DIR`.
   - Prevents arbitrary deletion outside the temporary folder.
4. **Session Fixation & CSRF (`app/auth/service.py`, `app/security/csrf.py`)**:
   - `rotate_session()` clears old session variables and regenerates session UUID on every successful admin login.
   - `require_csrf(request)` validates tokens on POST requests using constant-time comparison.
