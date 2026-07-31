#!/usr/bin/env python3
import argparse
import asyncio
import os
import sys
from pathlib import Path

# Add current dir to pythonpath
sys.path.insert(0, str(Path(__file__).parent.resolve()))

from app.admin.allowed_number_service import AllowedNumberService
from app.auth.service import hash_password
from app.config import get_settings
from app.database.connection import AsyncSessionLocal
from app.database.migrations import run_migrations
from app.database.models import Admin
from app.database.repositories import (
    AdminRepository,
    AllowedNumberRepository,
    JobRepository,
    UnmappedLidRepository,
)
from app.gateway.client import FarrosWAGatewayClient
from app.media.cleanup import check_disk_space, cleanup_expired_temp_files
from app.security.urls import normalize_phone_number
from sqlalchemy import text


async def init_db_cmd(args: argparse.Namespace) -> None:
    print("Initializing database & running migrations...")
    await run_migrations()
    print("✅ Database initialization completed successfully.")


async def create_admin_cmd(args: argparse.Namespace) -> None:
    await run_migrations()
    async with AsyncSessionLocal() as session:
        repo = AdminRepository(session)
        existing = await repo.get_by_username(args.username)
        pwd_hash = hash_password(args.password)
        if existing:
            existing.password_hash = pwd_hash
            if args.email:
                existing.email = args.email
            print(f"✅ Admin '{args.username}' updated successfully.")
        else:
            admin = Admin(
                username=args.username,
                password_hash=pwd_hash,
                email=args.email,
                is_active=True,
            )
            session.add(admin)
            print(f"✅ Admin '{args.username}' created successfully.")
        await session.commit()


async def reset_password_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        repo = AdminRepository(session)
        existing = await repo.get_by_username(args.username)
        if not existing:
            print(f"❌ Error: Admin '{args.username}' not found.", file=sys.stderr)
            sys.exit(1)
        existing.password_hash = hash_password(args.password)
        await session.commit()
        print(f"✅ Password for admin '{args.username}' reset successfully.")


async def list_numbers_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        repo = AllowedNumberRepository(session)
        numbers = await repo.list_numbers()
        if not numbers:
            print("No allowed numbers found in whitelist.")
            return
        print(f"{'ID':<5} {'Phone Number':<18} {'Name':<22} {'LID':<20} {'Active':<8} {'Jobs':<6}")
        print("-" * 85)
        for num in numbers:
            lid_str = num.lid_number or "-"
            print(
                f"{num.id:<5} {num.phone_number:<18} {num.name[:21]:<22} {lid_str:<20} {'Yes' if num.is_active else 'No':<8} {num.total_jobs:<6}"
            )


async def add_number_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        service = AllowedNumberService(session)
        lid_val = getattr(args, "lid", None)
        notes_val = getattr(args, "notes", None)
        num, err = await service.add_number(
            name=args.name, raw_phone=args.phone, raw_lid=lid_val, notes=notes_val
        )
        if err or not num:
            print(f"❌ Error: {err or 'Failed to add number'}", file=sys.stderr)
            sys.exit(1)
        print(f"✅ Number '{num.phone_number}' ({num.name}) added successfully.")




async def update_number_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        service = AllowedNumberService(session)
        norm_phone = normalize_phone_number(args.phone)
        if not norm_phone:
            print(f"❌ Error: Invalid phone number '{args.phone}'.", file=sys.stderr)
            sys.exit(1)

        existing = await service.number_repo.get_by_phone(norm_phone)
        if not existing:
            print(f"❌ Error: Phone number '{norm_phone}' not found in whitelist.", file=sys.stderr)
            sys.exit(1)

        num, err = await service.update_number(
            number_id=existing.id, name=args.name, raw_phone=norm_phone, raw_lid=args.lid, notes=args.notes
        )
        if err:
            print(f"❌ Error: {err}", file=sys.stderr)
            sys.exit(1)
        print(f"✅ Number '{norm_phone}' updated successfully.")


async def assign_lid_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        service = AllowedNumberService(session)
        norm_phone = normalize_phone_number(args.phone)
        if not norm_phone:
            print(f"❌ Error: Invalid phone number '{args.phone}'.", file=sys.stderr)
            sys.exit(1)

        existing = await service.number_repo.get_by_phone(norm_phone)
        if not existing:
            print(f"❌ Error: Phone number '{norm_phone}' not found in whitelist.", file=sys.stderr)
            sys.exit(1)

        num, err = await service.assign_lid(number_id=existing.id, raw_lid=args.lid)
        if err:
            print(f"❌ Error: {err}", file=sys.stderr)
            sys.exit(1)
        print(f"✅ LID '{args.lid}' assigned to '{norm_phone}' successfully.")


async def remove_lid_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        service = AllowedNumberService(session)
        norm_phone = normalize_phone_number(args.phone) or args.phone
        existing = await service.number_repo.get_by_phone(norm_phone)
        if not existing:
            print(f"❌ Error: Phone number '{norm_phone}' not found in whitelist.", file=sys.stderr)
            sys.exit(1)

        num, err = await service.remove_lid(number_id=existing.id)
        if err:
            print(f"❌ Error: {err}", file=sys.stderr)
            sys.exit(1)
        print(f"✅ LID mapping removed from '{norm_phone}'.")


async def toggle_number_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        service = AllowedNumberService(session)
        norm_phone = normalize_phone_number(args.phone) or args.phone
        existing = await service.number_repo.get_by_phone(norm_phone)
        if not existing:
            print(f"❌ Error: Phone number '{norm_phone}' not found in whitelist.", file=sys.stderr)
            sys.exit(1)

        num = await service.toggle_active(existing.id)
        if not num:
            print("❌ Error: Failed to toggle number status.", file=sys.stderr)
            sys.exit(1)
        status_str = "Active" if num.is_active else "Inactive"
        print(f"✅ Number '{norm_phone}' status toggled to '{status_str}'.")



async def remove_number_cmd(args: argparse.Namespace) -> None:
    norm_phone = normalize_phone_number(args.phone) or args.phone
    async with AsyncSessionLocal() as session:
        service = AllowedNumberService(session)
        existing = await service.number_repo.get_by_phone(norm_phone)
        if not existing:
            print(f"❌ Error: Phone number '{norm_phone}' not found in whitelist.", file=sys.stderr)
            sys.exit(1)

        await service.delete_number(existing.id)
        print(f"✅ Number '{norm_phone}' removed from whitelist.")


async def list_unmapped_lids_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        repo = UnmappedLidRepository(session)
        records, total = await repo.list_paginated(limit=100)
        if not records:
            print("No unmapped LIDs recorded.")
            return

        print(f"{'ID':<5} {'LID Number':<20} {'Count':<8} {'Last Seen':<20} {'Status':<12} {'Preview':<30}")
        print("-" * 100)
        for r in records:
            status_str = "Resolved" if r.resolved_at else "Unmapped"
            last_seen = r.last_seen_at.strftime("%Y-%m-%d %H:%M") if r.last_seen_at else "-"
            preview = (r.last_message_preview or "")[:28]
            print(f"{r.id:<5} {r.lid_number:<20} {r.occurrence_count:<8} {last_seen:<20} {status_str:<12} {preview:<30}")


async def retry_job_cmd(args: argparse.Namespace) -> None:
    async with AsyncSessionLocal() as session:
        repo = JobRepository(session)
        job = await repo.get_by_id(args.id)
        if not job:
            print(f"❌ Error: Job '{args.id}' not found.", file=sys.stderr)
            sys.exit(1)
        if job.status != "failed":
            print(f"❌ Error: Job status is '{job.status}'. Only 'failed' jobs can be retried.", file=sys.stderr)
            sys.exit(1)

        for item in job.items:
            if item.status == "failed":
                item.status = "pending"
                item.error_message = None

        job.status = "queued"
        job.error_code = None
        job.error_message = None
        job.attempt_count = 0
        await session.commit()
        print(f"✅ Job '{args.id}' requeued successfully.")


async def reconcile_gateway_cmd(args: argparse.Namespace) -> None:
    from datetime import timedelta

    from app.database.models import DownloadItem, utc_now
    from app.queue.reconciler import GatewayReconciler
    from sqlalchemy import select

    print(f"Reconciling gateway statuses for jobs within the last {args.days} days...")

    async with AsyncSessionLocal() as session:
        cutoff = utc_now() - timedelta(days=args.days)
        stmt = (
            select(DownloadItem)
            .where(
                DownloadItem.gateway_message_id.isnot(None),
                DownloadItem.created_at >= cutoff,
                DownloadItem.status.in_(["gateway_queued", "gateway_processing", "sent"]),
                DownloadItem.gateway_delivery_status.notin_(["read", "played", "delivered", "failed", "delivery_unknown"])
            )
        )
        res = await session.execute(stmt)
        items = res.scalars().all()

        if not items:
            print("No items to reconcile.")
            return

        print(f"Found {len(items)} items to reconcile. Syncing...")

    from app.database.connection import get_session_maker
    reconciler = GatewayReconciler(get_session_maker())
    reconciler.batch_size = len(items)
    await reconciler._reconcile_batch()

    print("✅ Gateway reconciliation completed.")


async def prune_temp_cmd(args: argparse.Namespace) -> None:
    settings = get_settings()
    ttl = args.ttl_minutes if args.ttl_minutes is not None else settings.TEMP_FILE_TTL_MINUTES
    removed = cleanup_expired_temp_files(ttl_minutes=ttl)
    print(f"✅ Pruned {removed} expired temporary items older than {ttl} minutes.")


async def check_health_cmd(args: argparse.Namespace) -> None:
    settings = get_settings()
    print("Checking system health status...")
    all_ok = True

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        print("  [OK] Database connection (SQLite WAL mode)")

    except Exception as e:
        print(f"  [FAIL] Database check failed: {e}")
        all_ok = False

    free_bytes = check_disk_space()
    free_gb = free_bytes / (1024 * 1024 * 1024)
    if free_gb >= 1.0:
        print(f"  [OK] Disk space free: {free_gb:.2f} GB")
    else:
        print(f"  [WARN] Low disk space: {free_gb:.2f} GB (< 1 GB)")
        if free_gb < 0.2:
            all_ok = False

    if os.path.exists(settings.YT_DLP_BINARY):
        print(f"  [OK] yt-dlp binary found at {settings.YT_DLP_BINARY}")
    else:
        import shutil

        if shutil.which(settings.YT_DLP_BINARY):
            print(f"  [OK] yt-dlp binary found in PATH ({settings.YT_DLP_BINARY})")
        else:
            print(f"  [FAIL] yt-dlp binary not found at '{settings.YT_DLP_BINARY}'")
            all_ok = False

    import shutil

    if os.path.exists(settings.FFMPEG_BINARY) or shutil.which(settings.FFMPEG_BINARY):
        print(f"  [OK] ffmpeg binary found ({settings.FFMPEG_BINARY})")
    else:
        print(f"  [FAIL] ffmpeg binary not found ({settings.FFMPEG_BINARY})")
        all_ok = False

    try:
        client = FarrosWAGatewayClient()
        import httpx

        async with httpx.AsyncClient(timeout=5.0) as http_client:
            resp = await http_client.get(client.base_url)
            print(f"  [OK] Farros WA Gateway reachability (HTTP {resp.status_code})")
    except Exception as e:
        print(f"  [WARN] Farros WA Gateway reachability check warning: {e}")

    if all_ok:
        print("\n✅ System is healthy and ready for production.")
    else:
        print("\n❌ One or more critical system checks failed.", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Farros Media Bot CLI Management Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init-db
    subparsers.add_parser("init-db", help="Initialize database & run migrations")

    # create-admin
    p_create = subparsers.add_parser("create-admin", help="Create or update an admin account")
    p_create.add_argument("--username", required=True, help="Admin username")
    p_create.add_argument("--password", required=True, help="Admin password")
    p_create.add_argument("--email", required=False, default=None, help="Admin email")

    # reset-password
    p_reset = subparsers.add_parser("reset-password", help="Reset password for existing admin")
    p_reset.add_argument("--username", required=True, help="Admin username")
    p_reset.add_argument("--password", required=True, help="New admin password")

    # list-numbers
    subparsers.add_parser("list-numbers", help="List all numbers in whitelist")

    # add-number
    p_add = subparsers.add_parser("add-number", help="Add a new number to whitelist")
    p_add.add_argument("--phone", required=True, help="Phone number (628xxx)")
    p_add.add_argument("--name", required=True, help="Owner name or division")
    p_add.add_argument("--lid", required=False, default=None, help="Optional WhatsApp LID digit number")
    p_add.add_argument("--notes", required=False, default=None, help="Optional notes")

    # update-number
    p_upd = subparsers.add_parser("update-number", help="Update an existing number in whitelist")
    p_upd.add_argument("--phone", required=True, help="Phone number to update")
    p_upd.add_argument("--name", required=True, help="Updated owner name")
    p_upd.add_argument("--lid", required=False, default=None, help="Updated LID")
    p_upd.add_argument("--notes", required=False, default=None, help="Updated notes")

    # assign-lid
    p_asgn = subparsers.add_parser("assign-lid", help="Assign an LID to an allowed phone number")
    p_asgn.add_argument("--phone", required=True, help="Phone number")
    p_asgn.add_argument("--lid", required=True, help="LID digit string")

    # remove-lid
    p_rmlid = subparsers.add_parser("remove-lid", help="Remove LID mapping from a phone number")
    p_rmlid.add_argument("--phone", required=True, help="Phone number")

    # toggle-number
    p_tgl = subparsers.add_parser("toggle-number", help="Toggle active status of a phone number")
    p_tgl.add_argument("--phone", required=True, help="Phone number")

    # remove-number
    p_rm = subparsers.add_parser("remove-number", help="Remove a number from whitelist")
    p_rm.add_argument("--phone", required=True, help="Phone number to remove")

    # list-unmapped-lids
    subparsers.add_parser("list-unmapped-lids", help="List all unmapped LID records")

    # retry-job
    p_retry = subparsers.add_parser("retry-job", help="Retry a failed download/send job")
    p_retry.add_argument("--id", required=True, help="Job UUID to retry")

    # prune-temp
    p_prune = subparsers.add_parser("prune-temp", help="Clean up expired temporary files")
    p_prune.add_argument("--ttl-minutes", type=int, default=None, help="TTL in minutes (defaults to config)")

    # check-health
    subparsers.add_parser("check-health", help="Run comprehensive health checks")

    # reconcile-gateway
    p_recon = subparsers.add_parser("reconcile-gateway", help="Sync gateway status for recent incomplete jobs")
    p_recon.add_argument("--days", type=int, default=7, help="Number of days to look back")

    args = parser.parse_args()

    commands = {
        "init-db": init_db_cmd,
        "create-admin": create_admin_cmd,
        "reset-password": reset_password_cmd,
        "list-numbers": list_numbers_cmd,
        "add-number": add_number_cmd,
        "update-number": update_number_cmd,
        "assign-lid": assign_lid_cmd,
        "remove-lid": remove_lid_cmd,
        "toggle-number": toggle_number_cmd,
        "remove-number": remove_number_cmd,
        "list-unmapped-lids": list_unmapped_lids_cmd,
        "retry-job": retry_job_cmd,
        "prune-temp": prune_temp_cmd,
        "check-health": check_health_cmd,
        "reconcile-gateway": reconcile_gateway_cmd,
    }

    if args.command in commands:
        asyncio.run(commands[args.command](args))


if __name__ == "__main__":
    main()
