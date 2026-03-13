# Ensure the virtualenv/site-packages directory is prioritized so installed
# packages are imported instead of local stub directories created earlier.
import sys
import site
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

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
