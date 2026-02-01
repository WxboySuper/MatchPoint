# src/api/deps.py
"""API dependencies (auth, database sessions, etc.)."""

import os
from fastapi import Header, HTTPException, status

API_KEY = os.getenv("MATCHPOINT_API_KEY", "")


async def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    """Verify the API key from request header."""
    if not API_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key not configured on server",
        )
    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    return x_api_key
