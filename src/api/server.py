# src/api/server.py
"""FastAPI application and server management."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from src.api.routes import router

logger = logging.getLogger("esports-bot.api")

API_PORT = int(os.getenv("MATCHPOINT_API_PORT", "8420"))
API_HOST = os.getenv("MATCHPOINT_API_HOST", "0.0.0.0")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="MatchPoint API",
        description="REST API for MatchPoint esports pick'em bot",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )
    app.include_router(router)
    return app


class APIServer:
    """Manages the uvicorn server lifecycle."""

    def __init__(self):
        self.server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None

    async def start(self):
        """Start the API server in a background task."""
        if not os.getenv("MATCHPOINT_API_KEY"):
            logger.warning(
                "MATCHPOINT_API_KEY not set. API will reject all requests."
            )

        app = create_app()
        config = uvicorn.Config(
            app,
            host=API_HOST,
            port=API_PORT,
            log_level="info",
            access_log=True,
        )
        self.server = uvicorn.Server(config)

        # Run server in background task
        self._task = asyncio.create_task(self.server.serve())
        logger.info("API server started on http://%s:%s", API_HOST, API_PORT)

    async def stop(self):
        """Stop the API server."""
        if self.server:
            self.server.should_exit = True
            if self._task:
                await self._task
            logger.info("API server stopped")


# Global server instance
api_server = APIServer()
