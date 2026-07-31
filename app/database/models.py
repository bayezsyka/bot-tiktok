import uuid
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Admin(Base):
    __tablename__ = "admins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)


class AllowedNumber(Base):
    __tablename__ = "allowed_numbers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    phone_number: Mapped[str] = mapped_column(String(30), unique=True, index=True, nullable=False)
    lid_number: Mapped[str | None] = mapped_column(String(50), unique=True, index=True, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    total_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)


class UnmappedLid(Base):
    __tablename__ = "unmapped_lids"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lid_number: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    last_inbound_message_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_message_preview: Mapped[str | None] = mapped_column(String(150), nullable=True)
    occurrence_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)


class DownloadJob(Base):
    __tablename__ = "download_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    inbound_message_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    webhook_event_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    sender_number: Mapped[str] = mapped_column(String(30), index=True, nullable=False)
    platform: Mapped[str] = mapped_column(String(20), default="tiktok", index=True, nullable=False)
    original_url: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(20), nullable=True)  # 'video' or 'photo'
    status: Mapped[str] = mapped_column(String(30), default="queued", index=True, nullable=False)
    media_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sent_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    source_size_bytes: Mapped[int | None] = mapped_column(Integer, default=0, nullable=True)
    final_size_bytes: Mapped[int | None] = mapped_column(Integer, default=0, nullable=True)
    duration_seconds: Mapped[int | None] = mapped_column(Integer, default=0, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gateway_status_summary: Mapped[str | None] = mapped_column(String(50), nullable=True)
    last_gateway_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_notification_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    items: Mapped[list["DownloadItem"]] = relationship("DownloadItem", back_populates="job", cascade="all, delete-orphan", order_by="DownloadItem.position", lazy="selectin")


class DownloadItem(Base):
    __tablename__ = "download_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("download_jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    position: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    media_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'video' or 'photo'
    status: Mapped[str] = mapped_column(String(30), default="pending", nullable=False)  # 'pending', 'processing', 'sent', 'failed'
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    local_filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_size_bytes: Mapped[int | None] = mapped_column(Integer, default=0, nullable=True)
    final_size_bytes: Mapped[int | None] = mapped_column(Integer, default=0, nullable=True)
    gateway_message_id: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    gateway_queue_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    gateway_delivery_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    gateway_error_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    gateway_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    gateway_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gateway_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gateway_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gateway_read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gateway_failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_gateway_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    job: Mapped["DownloadJob"] = relationship("DownloadJob", back_populates="items")


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SchemaMigration(Base):
    __tablename__ = "schema_migrations"

    version: Mapped[str] = mapped_column(String(50), primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
