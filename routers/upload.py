from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Header, Request
from fastapi.responses import FileResponse
from pathlib import Path
from typing import Optional
import json
import logging

from models.schemas import (
    UploadResponse,
    ProcessingResult,
    ValidationSummary,
    ColumnInfo,
    ValidationIssue,
    ChartData,
    DataStatus,
)
from utils.file_manager import (
    generate_job_id,
    validate_file_extension,
    validate_file_size,
    save_uploaded_file,
    get_job_dir,
    delete_job,
    MAX_FILE_SIZE_MB,
)
from utils.rate_limiter import upload_limiter, get_client_ip
from utils.serialization import convert_to_serializable
from services.parser import parse_file, get_column_info, ParserError
from services.cleaner import clean_dataframe
from services.validator import validate_dataframe, calculate_status
from services.visualizer import generate_charts
from services.reporter import generate_reports
from services.tier_manager import get_tier_from_token, apply_tier_limits, should_add_watermark, UserTier

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["upload"])

# In-memory job storage (for simplicity - in production use Redis)
job_store = {}


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    authorization: Optional[str] = Header(None),
):
    """Upload a CSV or Excel file for processing."""

    # Rate limiting
    client_ip = get_client_ip(request)
    if not await upload_limiter.check_rate_limit(client_ip):
        logger.warning(f"Rate limit exceeded for IP: {client_ip}")
        raise HTTPException(
            status_code=429,
            detail="Too many requests. Please wait a minute before uploading again.",
        )

    logger.info(f"Upload request from {client_ip}: {file.filename}")

    # Validate file extension
    if not validate_file_extension(file.filename):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .csv, .xlsx, and .xls files are allowed.",
        )

    # Read file content
    content = await file.read()

    # Validate file size
    if not validate_file_size(len(content)):
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size is {MAX_FILE_SIZE_MB}MB.",
        )

    # Determine user tier
    tier = get_tier_from_token(authorization)

    # Generate job ID and save file
    job_id = generate_job_id()
    file_path = save_uploaded_file(job_id, file.filename, content)

    # Initialize job status
    job_store[job_id] = {
        "status": "processing",
        "progress": 0,
        "message": "File uploaded, starting processing...",
        "filename": file.filename,
        "file_path": str(file_path),
        "tier": tier.value,
    }

    # Start background processing
    background_tasks.add_task(process_file, job_id, file_path, tier)

    return UploadResponse(
        job_id=job_id,
        filename=file.filename,
        message="File uploaded successfully. Processing started.",
    )


async def process_file(job_id: str, file_path: Path, tier: UserTier = UserTier.FREE):
    """Background task to process the uploaded file."""
    try:
        # Phase 1: Parse file
        job_store[job_id]["progress"] = 10
        job_store[job_id]["message"] = "Parsing file..."

        df, metadata = parse_file(file_path)

        # Apply tier limits
        df, limit_info = apply_tier_limits(df, tier)
        metadata["tier_info"] = limit_info

        # Phase 2: Clean data
        job_store[job_id]["progress"] = 30
        job_store[job_id]["message"] = "Cleaning data..."

        cleaned_df, cleaning_log = clean_dataframe(df)

        # Phase 3: Validate data
        job_store[job_id]["progress"] = 50
        job_store[job_id]["message"] = "Validating data..."

        issues = validate_dataframe(cleaned_df)
        summary = calculate_status(cleaned_df, issues, metadata)

        # Phase 4: Generate visualizations
        job_store[job_id]["progress"] = 70
        job_store[job_id]["message"] = "Generating charts..."

        charts = generate_charts(cleaned_df)

        # Phase 5: Generate reports
        job_store[job_id]["progress"] = 90
        job_store[job_id]["message"] = "Generating reports..."

        try:
            job_dir = get_job_dir(job_id)
            add_watermark = should_add_watermark(tier)
            generate_reports(cleaned_df, summary, issues, charts, job_dir, metadata, add_watermark)
        except Exception as e:
            logger.error(f"Report generation failed: {e}", exc_info=True)
            raise

        # Get column info
        columns_info = get_column_info(cleaned_df, metadata.get("column_mapping", {}))

        # Prepare preview data (first 100 rows) - convert numpy types to native Python
        preview_df = cleaned_df.head(100)
        preview_data = convert_to_serializable(preview_df.to_dict(orient="records"))

        # Store results - ensure all data is JSON serializable
        job_store[job_id]["status"] = "completed"
        job_store[job_id]["progress"] = 100
        job_store[job_id]["message"] = "Processing complete!"
        job_store[job_id]["result"] = {
            "validation_summary": summary,
            "columns": convert_to_serializable(columns_info),
            "issues": [issue.model_dump() for issue in issues],
            "charts": convert_to_serializable([chart.model_dump() for chart in charts]),
            "preview_data": preview_data,
            "cleaning_log": convert_to_serializable(cleaning_log),
            "tier_info": convert_to_serializable(limit_info),
        }

    except ParserError as e:
        job_store[job_id]["status"] = "error"
        job_store[job_id]["message"] = str(e)
    except Exception as e:
        import traceback
        logger.error(f"Processing failed: {traceback.format_exc()}")
        job_store[job_id]["status"] = "error"
        job_store[job_id]["message"] = f"Processing failed: {str(e)}"


@router.get("/status/{job_id}")
async def get_status(job_id: str):
    """Get the processing status of a job."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job = job_store[job_id]
    return {
        "job_id": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "message": job["message"],
    }


@router.get("/results/{job_id}")
async def get_results(job_id: str):
    """Get the processing results for a completed job."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job = job_store[job_id]

    if job["status"] == "processing":
        raise HTTPException(status_code=202, detail="Job still processing")

    if job["status"] == "error":
        raise HTTPException(status_code=500, detail=job["message"])

    # Convert validation_summary Pydantic model to dict and ensure serializable
    result = job["result"].copy()
    if hasattr(result.get("validation_summary"), "model_dump"):
        result["validation_summary"] = result["validation_summary"].model_dump()

    return convert_to_serializable({
        "job_id": job_id,
        **result,
    })


@router.get("/download/{job_id}/cleaned")
async def download_cleaned(job_id: str):
    """Download the cleaned dataset."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job_dir = get_job_dir(job_id)
    cleaned_file = job_dir / "cleaned_data.xlsx"

    if not cleaned_file.exists():
        raise HTTPException(status_code=404, detail="Cleaned file not found")

    return FileResponse(
        path=cleaned_file,
        filename="cleaned_data.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/download/{job_id}/report")
async def download_report(job_id: str):
    """Download the validation report."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job_dir = get_job_dir(job_id)
    report_file = job_dir / "validation_report.xlsx"

    if not report_file.exists():
        raise HTTPException(status_code=404, detail="Report file not found")

    return FileResponse(
        path=report_file,
        filename="validation_report.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.delete("/job/{job_id}")
async def delete_job_endpoint(job_id: str):
    """Delete a job and its associated files."""
    if job_id not in job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    delete_job(job_id)
    del job_store[job_id]

    return {"message": "Job deleted successfully"}
