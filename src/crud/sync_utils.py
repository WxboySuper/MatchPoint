import logging
from typing import List, Optional, Type, Any
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

logger = logging.getLogger(__name__)


async def _upsert_by_leaguepedia(
    session: AsyncSession,
    model: Type[Any],
    data: dict,
    update_keys: Optional[List[str]] = None,
) -> Optional[Any]:
    """
    Upserts a model instance identified by its `leaguepedia_id` using
    the provided data.

    Parameters:
        session (AsyncSession): Database session used for querying
            and persistence.
        model (Type[Any]): ORM model class that contains a
            `leaguepedia_id` field.
        data (dict): Mapping of field names to values; must include a
            `leaguepedia_id` key.
        update_keys (Optional[List[str]]): If provided, only these
            keys from `data` will be applied when updating an existing
            object; otherwise all fields (except `leaguepedia_id`) are
            applied.

    Returns:
        Optional[Any]: The created or updated model instance, or
            `None` if `leaguepedia_id` is missing or an error occurred
            during upsert.
    """
    leaguepedia_id = data.get("leaguepedia_id")
    if not leaguepedia_id:
        logger.error("Missing leaguepedia_id in data for %s", model.__name__)
        return None

    try:
        async with session.begin_nested():
            obj = await _find_existing_by_leaguepedia(
                session, model, leaguepedia_id
            )
            if obj is None:
                return await _create_new_by_leaguepedia(session, model, data)
            return await _update_existing_by_leaguepedia(
                session, obj, data, update_keys
            )
    except Exception:
        logger.exception(
            "Error upserting %s with data: %s",
            model.__name__,
            data,
        )
        return None


async def _find_existing_by_leaguepedia(
    session: AsyncSession, model: Type[Any], leaguepedia_id: str
) -> Optional[Any]:
    """
    Retrieve the first instance of the given ORM model with the
    specified Leaguepedia identifier.

    Parameters:
        model (Type[Any]): ORM model class that defines a
            `leaguepedia_id` attribute.
        leaguepedia_id (str): Leaguepedia identifier to match.

    Returns:
        Optional[Any]: The first matching model instance if found,
            `None` otherwise.
    """
    stmt = select(model).where(
        getattr(model, "leaguepedia_id", None) == leaguepedia_id
    )
    res = await session.exec(stmt)
    return res.first()


async def _create_new_by_leaguepedia(
    session: AsyncSession, model: Type[Any], data: dict
) -> Any:
    """
    Create and persist a new model instance initialized from the
    provided Leaguepedia data.

    Parameters:
        session (AsyncSession): Async database session used to add
            and flush the new instance.
        model (Type[Any]): ORM model class to instantiate.
        data (dict): Mapping of attributes used to construct the model
            instance.

    Returns:
        Any: The newly created and refreshed model instance.
    """
    logger.info("Creating new %s: %s", model.__name__, data.get("name"))
    obj = model(**data)
    session.add(obj)
    await session.flush()
    logger.info(
        "Upserted %s: %s (ID: %s)",
        obj.__class__.__name__,
        getattr(obj, "name", None),
        getattr(obj, "id", None),
    )
    return obj


async def _update_existing_by_leaguepedia(
    session: AsyncSession,
    obj: Any,
    data: dict,
    update_keys: Optional[List[str]] = None,
) -> Any:
    """
    Apply fields from `data` to an existing ORM model instance and
    persist the changes.

    Parameters:
        obj (Any): The existing model instance to update.
        data (dict): Mapping of field names to new values to apply to
            `obj`.
        update_keys (Optional[List[str]]): If provided, only keys in
            this list will be applied from `data`.

    Returns:
        Any: The refreshed model instance after updates have been
            flushed and reloaded from the database.
    """
    logger.info(
        "Updating existing %s: %s",
        obj.__class__.__name__,
        getattr(obj, "name", None),
    )

    _apply_updates_to_obj(obj, data, update_keys)

    session.add(obj)
    await session.flush()
    logger.info(
        "Upserted %s: %s (ID: %s)",
        obj.__class__.__name__,
        getattr(obj, "name", None),
        getattr(obj, "id", None),
    )
    return obj


def _apply_updates_to_obj(
    obj: Any, data: dict, update_keys: Optional[List[str]] = None
) -> None:
    """
    Apply fields from `data` to `obj`, optionally restricting updates
    to the keys listed in `update_keys`.

    Parameters:
        obj: The target object to receive updates.
        data (dict): Mapping of field names to new values.
        update_keys (Optional[List[str]]): If provided, only fields
            whose names appear in this list are applied; if `None`,
            all fields in `data` are applied.
    """
    if update_keys is None:
        _apply_all_updates(obj, data)
    else:
        _apply_selected_updates(obj, data, update_keys)


def _apply_all_updates(obj: Any, data: dict) -> None:
    """
    Apply every key/value in `data` to `obj` as attributes,
    excluding the `leaguepedia_id` key.

    Parameters:
        obj (Any): Target object whose attributes will be set.
        data (dict): Mapping of attribute names to values to apply;
            any `leaguepedia_id` entry is ignored.
    """
    for k, v in data.items():
        if k == "leaguepedia_id":
            continue
        setattr(obj, k, v)


def _apply_selected_updates(
    obj: Any, data: dict, update_keys: Optional[List[str]]
) -> None:
    """
    Apply selected fields from a data mapping to an object's attributes.

    Parameters:
        obj (Any): Target object whose attributes will be updated.
        data (dict): Mapping of attribute names to values.
        update_keys (Optional[List[str]]): List of keys to apply
            from `data`. If empty or None, no changes are made.

    Notes:
        Only keys present in both `update_keys` and `data` are
        applied; keys absent from `data` are ignored.
    """
    if not update_keys:
        return
    for k in update_keys:
        if k in data:
            setattr(obj, k, data[k])
