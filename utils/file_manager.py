import os
import uuid
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import shutil

UPLOAD_DIR = Path(__file__).parent.parent / "temp_uploads"
MAX_FILE_SIZE_MB = 10
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
FILE_EXPIRY_MINUTES = 30

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def ensure_upload_dir():
    """Ensure the upload directory exists."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def generate_job_id() -> str:
    """Generate a unique job ID."""
    return str(uuid.uuid4())


def get_job_dir(job_id: str) -> Path:
    """Get the directory for a specific job."""
    return UPLOAD_DIR / job_id


def create_job_dir(job_id: str) -> Path:
    """Create a directory for a job."""
    job_dir = get_job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def validate_file_extension(filename: str) -> bool:
    """Check if file has an allowed extension."""
    ext = Path(filename).suffix.lower()
    return ext in ALLOWED_EXTENSIONS


def validate_file_size(size: int) -> bool:
    """Check if file size is within limits."""
    return size <= MAX_FILE_SIZE_BYTES


def get_file_path(job_id: str, filename: str) -> Path:
    """Get the full path for an uploaded file."""
    job_dir = get_job_dir(job_id)
    return job_dir / filename


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal attacks."""
    # Remove any directory components
    filename = Path(filename).name
    # Remove any potentially dangerous characters
    filename = "".join(c for c in filename if c.isalnum() or c in '._-')
    # Ensure it has a valid extension
    if not filename:
        filename = "uploaded_file"
    return filename


def save_uploaded_file(job_id: str, filename: str, content: bytes) -> Path:
    """Save uploaded file to job directory."""
    job_dir = create_job_dir(job_id)
    # Sanitize filename to prevent path traversal
    safe_filename = sanitize_filename(filename)
    file_path = job_dir / safe_filename
    with open(file_path, "wb") as f:
        f.write(content)

    # Create timestamp file for cleanup tracking
    timestamp_file = job_dir / ".created_at"
    with open(timestamp_file, "w") as f:
        f.write(datetime.utcnow().isoformat())

    return file_path


def get_job_creation_time(job_id: str) -> Optional[datetime]:
    """Get when a job was created."""
    timestamp_file = get_job_dir(job_id) / ".created_at"
    if timestamp_file.exists():
        with open(timestamp_file, "r") as f:
            return datetime.fromisoformat(f.read().strip())
    return None


def delete_job(job_id: str):
    """Delete a job directory and all its contents."""
    job_dir = get_job_dir(job_id)
    if job_dir.exists():
        shutil.rmtree(job_dir)


def cleanup_expired_jobs():
    """Remove job directories older than FILE_EXPIRY_MINUTES."""
    ensure_upload_dir()
    expiry_threshold = datetime.utcnow() - timedelta(minutes=FILE_EXPIRY_MINUTES)

    for job_dir in UPLOAD_DIR.iterdir():
        if job_dir.is_dir():
            timestamp_file = job_dir / ".created_at"
            if timestamp_file.exists():
                with open(timestamp_file, "r") as f:
                    created_at = datetime.fromisoformat(f.read().strip())
                if created_at < expiry_threshold:
                    shutil.rmtree(job_dir)
            else:
                # If no timestamp, check directory modification time
                mtime = datetime.fromtimestamp(job_dir.stat().st_mtime)
                if mtime < expiry_threshold:
                    shutil.rmtree(job_dir)


async def schedule_cleanup():
    """Background task to periodically clean up expired files."""
    while True:
        await asyncio.sleep(300)  # Run every 5 minutes
        cleanup_expired_jobs()
