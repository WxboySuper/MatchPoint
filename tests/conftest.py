# Ensure the virtualenv/site-packages directory is prioritized so installed
# packages are imported instead of local stub directories created earlier.
import sys
import site
from unittest.mock import AsyncMock, MagicMock
from typing import Any

import discord
import pytest
from sqlmodel import Session

# Insert site-packages directories at the front of sys.path if available.
site_packages = [p for p in site.getsitepackages() if p not in sys.path]
for p in reversed(site_packages):
    sys.path.insert(0, p)

# Also add user site-packages
user_site = site.getusersitepackages()
if user_site and user_site not in sys.path:
    sys.path.insert(0, user_site)


@pytest.fixture
def mocked_interaction():
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.guild = MagicMock(id=123)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()
    return interaction


class _ResultWrapper:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows


class _AsyncSession:
    def __init__(self, engine: Any) -> None:
        self._engine = engine

    async def exec(self, stmt: Any) -> _ResultWrapper:
        with Session(self._engine) as session:
            return _ResultWrapper(list(session.exec(stmt).all()))

    async def __aenter__(self) -> "_AsyncSession":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        _ = (exc_type, exc, tb)
        return False


@pytest.fixture
def async_session_for_engine():
    def factory(engine: Any) -> _AsyncSession:
        return _AsyncSession(engine)

    return factory
