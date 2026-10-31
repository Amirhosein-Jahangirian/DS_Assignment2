"""
Gateway configuration, shard resolution, and replica health/selection state.

The gateway reads the same shard map as the banking servers. The map decides
ownership; gateway.yaml decides which replica URLs exist for each shard.

Replica selection is per-shard round-robin over replicas currently marked
healthy. A replica is ejected after two consecutive failed probes and rejoins
after three consecutive successful probes.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ACCOUNT_PATTERN = re.compile(r'^A\d{4}$')


class ConfigError(RuntimeError):
    """Raised when the shard map or gateway config is unusable."""


@dataclass(frozen=True)
class Range:
    start: str
    end: str
    shard_id: str


@dataclass
class Replica:
    id: str
    shard_id: str
    url: str
    healthy: bool = False
    initialized: bool = False
    consecutive_failures: int = 0
    consecutive_successes: int = 0


@dataclass
class GatewayConfig:
    ranges: tuple[Range, ...]
    replicas: dict[str, list[Replica]]
    health_interval: float
    health_timeout: float
    failures_to_unhealthy: int
    successes_to_healthy: int
    request_timeout: float
    _round_robin: dict[str, int] = field(default_factory=dict, repr=False)

    # -- routing ---------------------------------------------------------
    def shard_for(self, account_number: str) -> str | None:
        if not ACCOUNT_PATTERN.match(account_number or ''):
            return None
        for account_range in self.ranges:
            if account_range.start <= account_number <= account_range.end:
                return account_range.shard_id
        return None

    # -- replica selection ----------------------------------------------
    def select(self, shard_id: str, exclude: frozenset[str] = frozenset()) -> Replica | None:
        candidates = [
            replica
            for replica in self.replicas.get(shard_id, [])
            if replica.healthy and replica.id not in exclude
        ]
        if not candidates:
            return None

        index = self._round_robin.get(shard_id, 0)
        chosen = candidates[index % len(candidates)]
        self._round_robin[shard_id] = index + 1
        return chosen

    # -- health ----------------------------------------------------------
    def mark(self, replica: Replica, ok: bool) -> None:
        if ok:
            replica.consecutive_failures = 0
            replica.consecutive_successes += 1
            if not replica.initialized:
                # First successful probe makes a fresh replica usable
                # immediately instead of waiting for three successes.
                replica.healthy = True
            elif (
                not replica.healthy
                and replica.consecutive_successes >= self.successes_to_healthy
            ):
                replica.healthy = True
            replica.initialized = True
            return

        replica.consecutive_successes = 0
        replica.consecutive_failures += 1
        replica.initialized = True
        if (
            replica.healthy
            and replica.consecutive_failures >= self.failures_to_unhealthy
        ):
            replica.healthy = False

    def all_replicas(self) -> list[Replica]:
        return [replica for replicas in self.replicas.values() for replica in replicas]

    def snapshot(self) -> dict[str, dict[str, list[str]]]:
        return {
            shard_id: {
                'healthy': [r.id for r in replicas if r.healthy],
                'unhealthy': [r.id for r in replicas if not r.healthy],
            }
            for shard_id, replicas in self.replicas.items()
        }


def _load_yaml(path: Path) -> dict:
    try:
        with path.open('r', encoding='utf-8') as handle:
            data = yaml.safe_load(handle) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f'Configuration file not found: {path}') from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f'Invalid YAML in {path}: {exc}') from exc

    if not isinstance(data, dict):
        raise ConfigError(f'{path} must contain a mapping')
    return data


def _parse_ranges(map_data: dict) -> tuple[Range, ...]:
    raw_ranges = map_data.get('ranges')
    if not isinstance(raw_ranges, list) or not raw_ranges:
        raise ConfigError('shard_map.yaml must define a non-empty "ranges" list')

    ranges: list[Range] = []
    for item in raw_ranges:
        try:
            entry = Range(item['start'], item['end'], item['shard_id'])
        except (KeyError, TypeError) as exc:
            raise ConfigError(f'Malformed shard range: {item!r}') from exc

        if not (ACCOUNT_PATTERN.match(entry.start) and ACCOUNT_PATTERN.match(entry.end)):
            raise ConfigError(f'Invalid account boundary in range: {entry}')
        if entry.start > entry.end:
            raise ConfigError(f'Inverted account range: {entry}')
        ranges.append(entry)

    ranges.sort(key=lambda entry: entry.start)
    for previous, current in zip(ranges, ranges[1:]):
        if current.start <= previous.end:
            raise ConfigError(f'Overlapping ranges: {previous} and {current}')
        if f'A{int(previous.end[1:]) + 1:04d}' != current.start:
            raise ConfigError(f'Gap between ranges {previous} and {current}')

    return tuple(ranges)


def load_config(config_dir: str | None = None) -> GatewayConfig:
    base = Path(config_dir or os.environ.get('CONFIG_DIR', '/app/config'))
    map_data = _load_yaml(base / 'shard_map.yaml')
    gateway_data = _load_yaml(base / 'gateway.yaml')

    ranges = _parse_ranges(map_data)
    shard_ids = {entry.shard_id for entry in ranges}

    raw_replicas = gateway_data.get('replicas')
    if not isinstance(raw_replicas, dict) or not raw_replicas:
        raise ConfigError('gateway.yaml must define a non-empty "replicas" mapping')

    unknown = set(raw_replicas) - shard_ids
    if unknown:
        raise ConfigError(f'gateway.yaml defines unknown shards: {sorted(unknown)}')

    replicas: dict[str, list[Replica]] = {}
    seen_ids: set[str] = set()
    seen_urls: set[str] = set()

    for shard_id in shard_ids:
        entries = raw_replicas.get(shard_id)
        if not isinstance(entries, list) or not entries:
            raise ConfigError(f'No replicas configured for shard {shard_id!r}')

        replicas[shard_id] = []
        for entry in entries:
            try:
                replica_id = str(entry['id'])
                url = str(entry['url']).rstrip('/')
            except (KeyError, TypeError) as exc:
                raise ConfigError(f'Malformed replica entry: {entry!r}') from exc

            if not url.startswith(('http://', 'https://')):
                raise ConfigError(f'Replica URL must be absolute: {url!r}')
            if replica_id in seen_ids:
                raise ConfigError(f'Duplicate replica id: {replica_id!r}')
            if url in seen_urls:
                raise ConfigError(f'Duplicate replica URL: {url!r}')

            seen_ids.add(replica_id)
            seen_urls.add(url)
            replicas[shard_id].append(
                Replica(id=replica_id, shard_id=shard_id, url=url)
            )

    health = gateway_data.get('health') or {}
    return GatewayConfig(
        ranges=ranges,
        replicas=replicas,
        health_interval=float(health.get('interval_seconds', 2)),
        health_timeout=float(health.get('timeout_seconds', 1)),
        failures_to_unhealthy=int(health.get('failures_to_unhealthy', 2)),
        successes_to_healthy=int(health.get('successes_to_healthy', 3)),
        request_timeout=float(gateway_data.get('request_timeout_seconds', 5)),
    )
