from collections import defaultdict
from datetime import datetime, timedelta
from fastapi import HTTPException, Request
from functools import wraps
import asyncio

# Simple in-memory rate limiter
# In production, use Redis for distributed rate limiting
class RateLimiter:
    def __init__(self, requests_per_minute: int = 10):
        self.requests_per_minute = requests_per_minute
        self.requests = defaultdict(list)
        self._lock = asyncio.Lock()

    async def check_rate_limit(self, client_ip: str) -> bool:
        """Check if client has exceeded rate limit."""
        async with self._lock:
            now = datetime.utcnow()
            minute_ago = now - timedelta(minutes=1)

            # Clean old requests
            self.requests[client_ip] = [
                req_time for req_time in self.requests[client_ip]
                if req_time > minute_ago
            ]

            # Check limit
            if len(self.requests[client_ip]) >= self.requests_per_minute:
                return False

            # Record request
            self.requests[client_ip].append(now)
            return True

    async def cleanup(self):
        """Periodic cleanup of old entries."""
        while True:
            await asyncio.sleep(300)  # Every 5 minutes
            async with self._lock:
                now = datetime.utcnow()
                minute_ago = now - timedelta(minutes=1)
                for ip in list(self.requests.keys()):
                    self.requests[ip] = [
                        req_time for req_time in self.requests[ip]
                        if req_time > minute_ago
                    ]
                    if not self.requests[ip]:
                        del self.requests[ip]


# Global rate limiter instance
upload_limiter = RateLimiter(requests_per_minute=10)  # 10 uploads per minute per IP


def get_client_ip(request: Request) -> str:
    """Extract client IP from request."""
    # Check for forwarded headers (behind proxy)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"
