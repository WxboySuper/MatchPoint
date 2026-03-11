from typing import Optional
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models import GuildConfig
from src.db import get_session


def get_guild_config(session, guild_id: int) -> Optional[GuildConfig]:
    stmt = select(GuildConfig).where(GuildConfig.guild_id == guild_id)
    return session.exec(stmt).first()


def upsert_guild_config(session, guild_id: int, **fields) -> GuildConfig:
    cfg = get_guild_config(session, guild_id)
    if cfg is None:
        cfg = GuildConfig(guild_id=guild_id, **fields)
        session.add(cfg)
    else:
        for k, v in fields.items():
            setattr(cfg, k, v)
    session.commit()
    session.refresh(cfg)
    return cfg


def delete_guild_config(session, guild_id: int) -> None:
    cfg = get_guild_config(session, guild_id)
    if cfg:
        session.delete(cfg)
        session.commit()


# Async helpers for runtime (asyncio) code paths
async def get_guild_config_async(session: AsyncSession, guild_id: int) -> Optional[GuildConfig]:
    stmt = select(GuildConfig).where(GuildConfig.guild_id == guild_id)
    res = await session.exec(stmt)
    return res.first()


async def upsert_guild_config_async(session: AsyncSession, guild_id: int, **fields) -> GuildConfig:
    cfg = await get_guild_config_async(session, guild_id)
    if cfg is None:
        cfg = GuildConfig(guild_id=guild_id, **fields)
        session.add(cfg)
        await session.commit()
        await session.refresh(cfg)
        return cfg
    for k, v in fields.items():
        setattr(cfg, k, v)
    await session.commit()
    await session.refresh(cfg)
    return cfg


async def delete_guild_config_async(session: AsyncSession, guild_id: int) -> None:
    cfg = await get_guild_config_async(session, guild_id)
    if cfg:
        await session.delete(cfg)
        await session.commit()
