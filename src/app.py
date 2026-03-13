"""
src/app.py - Minimal Discord bot using app_commands (slash commands).
Environment variables used:
- DISCORD_TOKEN
- DEVELOPER_GUILD_ID (optional; for fast dev guild command registration)
- LOG_LEVEL (optional; DEBUG/INFO/WARNING/ERROR)
- ADMIN_IDS (optional, comma-separated user IDs)
"""

import logging
import os
import sys
import importlib
import inspect
import discord
from discord.ext import commands
from dotenv import load_dotenv, find_dotenv
from src.logging_config import setup_logging
from src.scheduler import start_scheduler
from src.bot_instance import set_bot_instance
import aiohttp
from src.db import init_db

# Initialize logging as early as possible
setup_logging()

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

logger.info("Starting up bot...")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.members = True


class EsportsBot(commands.Bot):
    COMMAND_MODULES = [
        "config",
        "ping",
        "info",
        "contest",
        "matches",
        "pick",
        "picks",
        "stats",
        "leaderboard",
        "result",
        "announce",
        "wipe",
        "sync_matches",
    ]

    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.session = None

    async def setup_hook(self):
        """
        Run asynchronous startup tasks required before the bot
        becomes ready.

        Initializes the HTTP session, registers the global bot
        instance, ensures database tables exist, starts the scheduler,
        loads command modules (if a commands package is available), and
        synchronizes global application commands. Exceptions raised during
        database initialization are logged and re-raised, causing the startup
        process to abort.
        """
        logger.info("Executing setup_hook...")
        self.session = aiohttp.ClientSession()
        set_bot_instance(self)
        # Ensure DB tables exist before the scheduler or commands hit the DB
        try:
            logger.info("Ensuring database tables exist...")
            init_db()
        except Exception:
            logger.exception("Failed initializing database tables.")
            raise
        logger.info("Starting scheduler...")
        start_scheduler()
        logger.info("Loading command modules...")
        commands_pkg = self._resolve_commands_package()
        if commands_pkg is not None:
            await self._load_command_modules(commands_pkg)
        await self._sync_global_commands()
        logger.info("setup_hook complete.")

    async def close(self):
        """Properly close the HTTP session and the bot."""
        from src.pandascore_client import pandascore_client

        if self.session:
            await self.session.close()
        await pandascore_client.close()
        await super().close()

    @staticmethod
    def _resolve_commands_package():
        if __package__:
            pkg_name = f"{__package__}.commands"
            try:
                return importlib.import_module(pkg_name)
            except ImportError:
                logger.debug(
                    "Package %s not found: %s",
                    pkg_name,
                    sys.path[:6],
                )

        try:
            return importlib.import_module("commands")
        except ImportError:
            logger.error(
                "Could not import 'commands' package; no commands loaded. "
                "Ensure either: (1) you run the app as a module "
                "or (2) /path/to/src is on PYTHONPATH, or change imports "
                "to be relative."
            )
            return None

    async def _load_command_modules(self, commands_pkg):
        for name in self.COMMAND_MODULES:
            full_name = f"{commands_pkg.__name__}.{name}"
            try:
                mod = importlib.import_module(full_name)
                setup_fn = getattr(mod, "setup", None)
                if setup_fn is None:
                    logger.debug(
                        "Module %s has no setup(bot); skipping",
                        full_name,
                    )
                    continue
                if inspect.iscoroutinefunction(setup_fn):
                    await setup_fn(self)
                else:
                    setup_fn(self)
                logger.info("Loaded command module: %s", full_name)
            except Exception:
                logger.exception("Failed loading command module %s", full_name)

    async def _sync_global_commands(self):
        try:
            logger.info("Starting Global Sync...")
            await self.tree.sync()
            logger.info("Performed GLOBAL command sync.")
        except Exception as exc:
            logger.exception("Failed performing global command sync: %s", exc)


bot = EsportsBot()


@bot.event
async def on_ready():
    logger.info("Bot is ready.")
    logger.info("Logged in as %s (id=%s)", bot.user, bot.user.id)


@bot.event
async def on_error(event, *args, **kwargs):
    logger.exception("Unhandled exception in event %s", event)


def main():
    if not DISCORD_TOKEN:
        logger.error("DISCORD_TOKEN not set. Exiting.")
        sys.exit(1)
    try:
        bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    except Exception:
        logger.exception("Bot terminated unexpectedly.")
        raise


if __name__ == "__main__":
    main()
