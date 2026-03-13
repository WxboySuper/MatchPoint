import logging
from datetime import datetime
from src.models import Match
from src.notification_batcher import batcher

logger = logging.getLogger(__name__)


async def send_result_notification(match_id: int, result_id: int):
    """
    Queues a result notification to be sent via the notification batcher.
    """
    logger.info("Queuing result notification for match %s", match_id)
    await batcher.add_result(match_id, result_id)


async def send_mid_series_update(match: Match, score: str):
    """
    Queues a mid-series update notification to be sent via the notification
    batcher.
    """
    logger.info("Queuing mid-series update for match %s", match.id)
    # Route mid-series updates to the batcher; batcher will determine
    # whether to broadcast as edits to a tracked message or as embeds.
    await batcher.add_mid_series_update(match.id, score)


async def send_match_time_change_notification(
    match: Match, old_time: datetime, new_time: datetime
):
    """
    Queues a time change notification to be sent via the notification batcher.
    """
    logger.info("Queuing time change notification for match %s", match.id)
    await batcher.add_time_change(match.id, old_time, new_time)
