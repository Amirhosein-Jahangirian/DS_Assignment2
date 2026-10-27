"""
Shard map loading, validation, and account ownership checks.

The map is the single source of truth for which shard owns which account
range. It is read once per process and validated at application startup, so a
replica cannot boot with an unknown SHARD_ID or a malformed map.
"""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

import yaml
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.models import Q

ACCOUNT_PATTERN = re.compile(r'^A\d{4}$')


class ShardRange(NamedTuple):
    start: str
    end: str
    shard_id: str


def _read_map(path: Path) -> dict:
    try:
        with path.open('r', encoding='utf-8') as handle:
            data = yaml.safe_load(handle) or {}
    except FileNotFoundError as exc:
        raise ImproperlyConfigured(f'Shard map not found: {path}') from exc
    except yaml.YAMLError as exc:
        raise ImproperlyConfigured(f'Invalid YAML in shard map {path}: {exc}') from exc

    if not isinstance(data, dict):
        raise ImproperlyConfigured(f'Shard map {path} must contain a mapping')

    return data


@lru_cache(maxsize=1)
def shard_map() -> dict:
    """Return the parsed map with validated ranges (cached per process)."""
    data = _read_map(Path(settings.SHARD_MAP_PATH))
    raw_ranges = data.get('ranges')

    if not isinstance(raw_ranges, list) or not raw_ranges:
        raise ImproperlyConfigured('Shard map must define a non-empty "ranges" list')

    ranges: list[ShardRange] = []
    for item in raw_ranges:
        try:
            entry = ShardRange(item['start'], item['end'], item['shard_id'])
        except (KeyError, TypeError) as exc:
            raise ImproperlyConfigured(f'Malformed shard range: {item!r}') from exc

        if not (ACCOUNT_PATTERN.match(entry.start) and ACCOUNT_PATTERN.match(entry.end)):
            raise ImproperlyConfigured(f'Invalid account boundary in range: {entry}')
        if entry.start > entry.end:
            raise ImproperlyConfigured(f'Inverted account range: {entry}')

        ranges.append(entry)

    ranges.sort(key=lambda entry: entry.start)
    for previous, current in zip(ranges, ranges[1:]):
        if current.start <= previous.end:
            raise ImproperlyConfigured(
                f'Overlapping ranges: {previous} and {current}'
            )
        if _next_account(previous.end) != current.start:
            raise ImproperlyConfigured(
                f'Gap between ranges {previous} and {current}'
            )

    return {
        'version': data.get('version', 1),
        'epoch': data.get('epoch', 1),
        'ranges': tuple(ranges),
    }


def _next_account(account_number: str) -> str:
    return f'A{int(account_number[1:]) + 1:04d}'


def ranges_for_shard(shard_id: str | None = None) -> tuple[ShardRange, ...]:
    """Ranges owned by a shard; defaults to this process's SHARD_ID."""
    target = shard_id or settings.SHARD_ID
    owned = tuple(r for r in shard_map()['ranges'] if r.shard_id == target)
    if not owned:
        raise ImproperlyConfigured(
            f'SHARD_ID={target!r} does not own any range in {settings.SHARD_MAP_PATH}'
        )
    return owned


def owns(account_number: str, shard_id: str | None = None) -> bool:
    """True when the account is inside one of the shard's ranges."""
    if not ACCOUNT_PATTERN.match(account_number or ''):
        return False
    return any(r.start <= account_number <= r.end for r in ranges_for_shard(shard_id))


def account_filter(shard_id: str | None = None) -> Q:
    """Queryset filter matching every account owned by the shard."""
    query = Q(pk__in=[])
    for account_range in ranges_for_shard(shard_id):
        query |= Q(
            account_number__gte=account_range.start,
            account_number__lte=account_range.end,
        )
    return query


def validate() -> None:
    """Fail fast when the map or this process's SHARD_ID is unusable."""
    shard_map()
    ranges_for_shard()
