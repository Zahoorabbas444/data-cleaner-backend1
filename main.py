from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager

from routers.upload import router as upload_router
from routers.payment import router as payment_router
from utils.file_manager import ensure_upload_dir, schedule_cleanup, cleanup_expired_jobs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    ensure_upload_dir()
    cleanup_expired_jobs()  # Clean any leftover files from previous runs

    # Start background cleanup task
    cleanup_task = asyncio.create_task(schedule_cleanup())

    yield

    # Shutdown
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Data Cleaner & Visual Insight Tool",
    description="Upload, clean, validate, and visualize your data with ease.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(upload_router)
app.include_router(payment_router)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Data Cleaner & Visual Insight Tool API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
