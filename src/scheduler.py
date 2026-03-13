import logging
from src.scheduler_instance import scheduler

logger = logging.getLogger(__name__)


def start_scheduler():
    """
    Ensure the module scheduler has the required recurring jobs and start
    it if not already running.

    Registers a 1-hour interval job to sync PandaScore data and a
    1-minute interval job to poll running matches, then starts the
    scheduler if it is not running. If the scheduler is already running,
    the function leaves it unchanged.
    """
    from src.pandascore_sync import perform_pandascore_sync
    from src.pandascore_polling import poll_running_matches_job
    from src.config import DEFAULT_GAMES
    from src.notification_batcher import update_upcoming_live_messages

    if not getattr(scheduler, "running", False):
        logger.info("Scheduler not running. Starting jobs...")
        # Schedule sync jobs for each configured default game. For
        # backward-compatibility keep the legacy job id 'sync_pandascore_job'
        # pointing to the first/default game.
        default_games = DEFAULT_GAMES or ["lol"]
        first_game = default_games[0]
        scheduler.add_job(
            perform_pandascore_sync,
            "interval",
            hours=1,
            id="sync_pandascore_job",
            replace_existing=True,
            kwargs={"game": first_game},
        )
        logger.info(
            "Added 'sync_pandascore_job' (game=%s) to scheduler.", first_game
        )

        # Additional per-game jobs (if multiple defaults are configured)
        for g in default_games[1:]:
            job_id = f"sync_pandascore_job_{g}"
            scheduler.add_job(
                perform_pandascore_sync,
                "interval",
                hours=1,
                id=job_id,
                replace_existing=True,
                kwargs={"game": g},
            )
            logger.info("Added '%s' (game=%s) to scheduler.", job_id, g)

        # Poll running matches every 1 minute for score updates
        scheduler.add_job(
            poll_running_matches_job,
            "interval",
            minutes=1,
            id="poll_running_matches_job",
            replace_existing=True,
        )
        logger.info("Added 'poll_running_matches_job' to scheduler.")

        # Rebuild upcoming live messages periodically (idempotent)
        scheduler.add_job(
            update_upcoming_live_messages,
            "interval",
            minutes=5,
            id="update_upcoming_live_messages_job",
            replace_existing=True,
        )
        logger.info("Added 'update_upcoming_live_messages_job' to scheduler.")

        scheduler.start()
        logger.info("Scheduler started.")
    else:
        logger.info("Scheduler is already running.")
