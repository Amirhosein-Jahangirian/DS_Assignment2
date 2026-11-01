"""
Durable two-phase-commit coordinator.

The coordinator shares the gateway container (the assignment explicitly allows
this as long as the responsibilities stay separate) but keeps its decision log
in a dedicated PostgreSQL database. Keeping the log off the shard databases is
what lets a recorded COMMIT survive a shard outage and be completed later.

Key invariants:

* COMMIT_DECIDED is durable before any COMMIT PREPARED is sent.
* An existing decision is never overwritten.
* COMPLETED is only written after both participants acknowledge COMMIT.
* Recovery never changes a COMMIT decision into an ABORT.
* Only one gateway/coordinator process is assumed; a per-tx in-process lock
  prevents the request path and the recovery loop from racing on one row.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable
from urllib.parse import quote

import asyncpg
import httpx

from balancer import ConfigError, GatewayConfig

logger = logging.getLogger('gateway.coordinator')

TXID_PATTERN = re.compile(r'^[A-Za-z0-9_-]{8,128}$')

# Coordinator statements touch a single row and a healthy database answers in
# milliseconds. These bounds keep a dead coordinator database from turning into
# a slow request: without them asyncpg's acquire timeout, command timeout and
# the retry all stack up before the API can report 503.
STORE_OPERATION_TIMEOUT_SECONDS = 4.0
HEALTH_TIMEOUT_SECONDS = 2.0

TERMINAL_STATES = frozenset({'COMPLETED', 'ABORTED'})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS coordinator_transactions (
    txid TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount NUMERIC(15, 2) NOT NULL CHECK (amount > 0),
    from_shard TEXT NOT NULL,
    to_shard TEXT NOT NULL,
    state TEXT NOT NULL,
    decision TEXT,
    error_code TEXT,
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CHECK (from_account <> to_account),
    CHECK (state IN (
        'INITIATED', 'PREPARING', 'PREPARED',
        'COMMIT_DECIDED', 'ABORT_DECIDED', 'COMPLETED', 'ABORTED'
    )),
    CHECK (decision IS NULL OR decision IN ('COMMIT', 'ABORT')),
    CHECK (
        (state IN ('INITIATED', 'PREPARING', 'PREPARED') AND decision IS NULL)
        OR (state IN ('COMMIT_DECIDED', 'COMPLETED') AND decision = 'COMMIT')
        OR (state IN ('ABORT_DECIDED', 'ABORTED') AND decision = 'ABORT')
    )
)
"""

_INDEX = """
CREATE INDEX IF NOT EXISTS coordinator_transactions_unresolved_idx
    ON coordinator_transactions (created_at)
    WHERE state NOT IN ('COMPLETED', 'ABORTED')
"""


class CoordinatorError(Exception):
    """A coordinator failure the API can translate into a response."""

    def __init__(self, code: str, message: str, *, http_status: int = 500):
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(message)


@dataclass(frozen=True)
class CoordinatorSettings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    rpc_timeout: float
    rpc_attempts: int
    recovery_interval: float
    preparing_grace: float
    pause_after_prepare: float
    pause_after_decision: float

    @classmethod
    def from_env(cls) -> 'CoordinatorSettings':
        def float_env(name: str, default: float, minimum: float) -> float:
            raw = os.environ.get(name, str(default))
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise ConfigError(f'{name} must be a number, got {raw!r}') from exc
            if value < minimum:
                raise ConfigError(f'{name} must be >= {minimum}, got {value}')
            return value

        def int_env(name: str, default: int, minimum: int) -> int:
            raw = os.environ.get(name, str(default))
            try:
                value = int(raw)
            except (TypeError, ValueError) as exc:
                raise ConfigError(f'{name} must be an integer, got {raw!r}') from exc
            if value < minimum:
                raise ConfigError(f'{name} must be >= {minimum}, got {value}')
            return value

        db_host = os.environ.get('COORD_DB_HOST')
        if not db_host:
            raise ConfigError('COORD_DB_HOST is required for the coordinator')

        db_user = os.environ.get('COORD_DB_USER') or os.environ.get('DB_USER')
        if not db_user:
            raise ConfigError('COORD_DB_USER (or DB_USER) is required')

        db_password = os.environ.get('COORD_DB_PASSWORD')
        if db_password is None:
            db_password = os.environ.get('DB_PASSWORD')
        if db_password is None:
            raise ConfigError('COORD_DB_PASSWORD (or DB_PASSWORD) is required')

        return cls(
            db_host=db_host,
            db_port=int_env('COORD_DB_PORT', 5432, 1),
            db_name=os.environ.get('COORD_DB_NAME', 'banking_coord'),
            db_user=db_user,
            db_password=db_password,
            rpc_timeout=float_env('COORD_RPC_TIMEOUT_SECONDS', 6.0, 0.1),
            rpc_attempts=int_env('COORD_RPC_ATTEMPTS', 2, 1),
            recovery_interval=float_env(
                'COORD_RECOVERY_INTERVAL_SECONDS', 5.0, 0.1
            ),
            preparing_grace=float_env('COORD_PREPARING_GRACE_SECONDS', 12.0, 0.0),
            pause_after_prepare=float_env(
                'COORD_TEST_PAUSE_AFTER_PREPARE_SECONDS', 0.0, 0.0
            ),
            pause_after_decision=float_env(
                'COORD_TEST_PAUSE_AFTER_DECISION_SECONDS', 0.0, 0.0
            ),
        )


class TxLocks:
    """
    Reference-counted per-transaction locks.

    A plain dict of locks would either leak one entry per transfer or remove a
    lock while another coroutine is waiting on it. The refcount keeps the same
    lock object alive until the last holder and waiter are gone.
    """

    def __init__(self) -> None:
        self._entries: dict[str, list[Any]] = {}

    @asynccontextmanager
    async def hold(self, txid: str):
        entry = self._entries.get(txid)
        if entry is None:
            entry = [asyncio.Lock(), 0]
            self._entries[txid] = entry
        entry[1] += 1
        try:
            async with entry[0]:
                yield
        finally:
            entry[1] -= 1
            if entry[1] == 0:
                self._entries.pop(txid, None)


class CoordinatorStore:
    """PostgreSQL-backed decision log for the coordinator."""

    def __init__(self, settings: CoordinatorSettings):
        self.settings = settings
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            host=self.settings.db_host,
            port=self.settings.db_port,
            database=self.settings.db_name,
            user=self.settings.db_user,
            password=self.settings.db_password,
            min_size=1,
            max_size=10,
            timeout=2,
            command_timeout=2,
        )
        try:
            async with self._pool.acquire() as connection:
                await connection.execute(_SCHEMA, timeout=10)
                await connection.execute(_INDEX, timeout=10)
        except Exception:
            await self._pool.close()
            self._pool = None
            raise

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _with_connection(
        self, operation: Callable[[asyncpg.Connection], Awaitable[Any]]
    ) -> Any:
        if self._pool is None:
            raise CoordinatorError(
                'COORDINATOR_UNAVAILABLE',
                'The coordinator state store is not connected',
                http_status=503,
            )

        try:
            return await asyncio.wait_for(
                self._attempt_operation(operation),
                timeout=STORE_OPERATION_TIMEOUT_SECONDS,
            )
        except (asyncio.TimeoutError, TimeoutError) as exc:
            logger.warning(
                'coordinator database operation timed out after %ss',
                STORE_OPERATION_TIMEOUT_SECONDS,
            )
            raise CoordinatorError(
                'COORDINATOR_UNAVAILABLE',
                'The coordinator state store is unavailable',
                http_status=503,
            ) from exc

    async def _attempt_operation(
        self, operation: Callable[[asyncpg.Connection], Awaitable[Any]]
    ) -> Any:
        last_error: BaseException | None = None
        for attempt in (1, 2):
            try:
                async with self._pool.acquire() as connection:
                    return await operation(connection)
            except (
                asyncpg.PostgresConnectionError,
                asyncpg.InterfaceError,
                OSError,
            ) as exc:
                last_error = exc
                if attempt == 2:
                    break
                logger.warning(
                    'coordinator database connection lost; reconnecting: %s', exc
                )
                expired = self._pool.expire_connections()
                if inspect.isawaitable(expired):
                    await expired

        raise CoordinatorError(
            'COORDINATOR_UNAVAILABLE',
            'The coordinator state store is unavailable',
            http_status=503,
        ) from last_error

    async def health(self) -> dict[str, Any]:
        if self._pool is None:
            raise CoordinatorError(
                'COORDINATOR_UNAVAILABLE',
                'The coordinator state store is not connected',
                http_status=503,
            )
        try:
            return await asyncio.wait_for(
                self._health_check(),
                timeout=HEALTH_TIMEOUT_SECONDS,
            )
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise CoordinatorError(
                'COORDINATOR_UNAVAILABLE',
                'The coordinator state store is unavailable',
                http_status=503,
            ) from exc

    async def _health_check(self) -> dict[str, Any]:
        async with self._pool.acquire() as connection:
            await connection.fetchval('SELECT 1', timeout=1)
            unresolved = await connection.fetchval(
                "SELECT count(*) FROM coordinator_transactions "
                "WHERE state NOT IN ('COMPLETED', 'ABORTED')",
                timeout=1,
            )
        return {'db': 'ok', 'unresolved': int(unresolved)}

    async def create(
        self,
        *,
        txid: str,
        request_id: str | None,
        from_account: str,
        to_account: str,
        amount: Decimal,
        from_shard: str,
        to_shard: str,
    ) -> dict[str, Any] | None:
        """
        Insert a new transaction.

        Returns None when the request_id is already in use. A retry of the same
        txid after a lost connection is resolved by the follow-up get().
        """
        try:
            row = await self._with_connection(
                lambda connection: connection.fetchrow(
                    """
                    INSERT INTO coordinator_transactions (
                        txid, request_id, from_account, to_account, amount,
                        from_shard, to_shard, state
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'INITIATED')
                    ON CONFLICT (txid) DO NOTHING
                    RETURNING *
                    """,
                    txid,
                    request_id,
                    from_account,
                    to_account,
                    amount,
                    from_shard,
                    to_shard,
                )
            )
        except asyncpg.UniqueViolationError:
            return None

        if row is not None:
            return dict(row)
        return await self.get(txid)

    async def get(self, txid: str) -> dict[str, Any] | None:
        row = await self._with_connection(
            lambda connection: connection.fetchrow(
                'SELECT * FROM coordinator_transactions WHERE txid = $1',
                txid,
            )
        )
        return dict(row) if row is not None else None

    async def find_by_request_id(
        self, request_id: str
    ) -> dict[str, Any] | None:
        row = await self._with_connection(
            lambda connection: connection.fetchrow(
                'SELECT * FROM coordinator_transactions WHERE request_id = $1',
                request_id,
            )
        )
        return dict(row) if row is not None else None

    async def unresolved(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self._with_connection(
            lambda connection: connection.fetch(
                """
                SELECT * FROM coordinator_transactions
                WHERE state NOT IN ('COMPLETED', 'ABORTED')
                ORDER BY created_at
                LIMIT $1
                """,
                limit,
            )
        )
        return [dict(row) for row in rows]

    async def mark_preparing(self, txid: str) -> dict[str, Any] | None:
        return await self._transition(
            """
            UPDATE coordinator_transactions
            SET state = 'PREPARING', updated_at = now()
            WHERE txid = $1 AND state = 'INITIATED' AND decision IS NULL
            RETURNING *
            """,
            txid,
        )

    async def mark_prepared(self, txid: str) -> dict[str, Any] | None:
        return await self._transition(
            """
            UPDATE coordinator_transactions
            SET state = 'PREPARED', updated_at = now()
            WHERE txid = $1 AND state = 'PREPARING' AND decision IS NULL
            RETURNING *
            """,
            txid,
        )

    async def record_decision(
        self,
        txid: str,
        decision: str,
        state: str,
        error_code: str | None = None,
        message: str | None = None,
    ) -> dict[str, Any] | None:
        if (decision, state) not in {
            ('COMMIT', 'COMMIT_DECIDED'),
            ('ABORT', 'ABORT_DECIDED'),
        }:
            raise ValueError(f'Invalid decision transition: {decision}/{state}')

        return await self._transition(
            """
            UPDATE coordinator_transactions
            SET state = $2, decision = $3, error_code = $4, message = $5,
                updated_at = now()
            WHERE txid = $1 AND decision IS NULL
            RETURNING *
            """,
            txid,
            state,
            decision,
            error_code,
            message,
        )

    async def mark_terminal(
        self, txid: str, state: str, expected: str
    ) -> dict[str, Any] | None:
        if (state, expected) not in {
            ('COMPLETED', 'COMMIT_DECIDED'),
            ('ABORTED', 'ABORT_DECIDED'),
        }:
            raise ValueError(f'Invalid terminal transition: {expected} -> {state}')

        return await self._transition(
            """
            UPDATE coordinator_transactions
            SET state = $2, updated_at = now(), completed_at = now()
            WHERE txid = $1 AND state = $3 AND decision IS NOT NULL
            RETURNING *
            """,
            txid,
            state,
            expected,
        )

    async def _transition(self, query: str, *args: Any) -> dict[str, Any] | None:
        row = await self._with_connection(
            lambda connection: connection.fetchrow(query, *args)
        )
        if row is not None:
            return dict(row)
        # The guard in the UPDATE did not match; report the current row so the
        # caller can see whether another path already advanced the state.
        txid = args[0]
        return await self.get(txid)


@dataclass
class RpcOutcome:
    ok: bool
    status_code: int
    body: dict[str, Any]
    replica_id: str | None = None
    error: str | None = None

    @property
    def error_code(self) -> str | None:
        value = self.body.get('error_code')
        return str(value) if value else None

    @property
    def message(self) -> str | None:
        value = self.body.get('message')
        return str(value) if value else None


class Coordinator:
    """Runs 2PC using the durable state store and the gateway's replica pool."""

    def __init__(
        self,
        config: GatewayConfig,
        store: CoordinatorStore,
        settings: CoordinatorSettings,
        client: httpx.AsyncClient,
    ):
        self.config = config
        self.store = store
        self.settings = settings
        self.client = client
        self.locks = TxLocks()
        self._recovery_paused = False

    # -- public entry point ------------------------------------------------
    async def submit(
        self,
        *,
        from_account: str,
        to_account: str,
        amount: Decimal,
        from_shard: str,
        to_shard: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        txid = uuid.uuid4().hex

        async with self.locks.hold(txid):
            row = await self.store.create(
                txid=txid,
                request_id=request_id,
                from_account=from_account,
                to_account=to_account,
                amount=amount,
                from_shard=from_shard,
                to_shard=to_shard,
            )

            if row is None:
                existing = None
                if request_id is not None:
                    existing = await self.store.find_by_request_id(request_id)
                if existing is None:
                    existing = await self.store.get(txid)
                if existing is None:
                    raise CoordinatorError(
                        'COORDINATOR_ERROR',
                        'Could not create the transaction record',
                    )
                self._check_replay(existing, from_account, to_account, amount)
                return existing

            return await self._advance_locked(row, wait=True)

    # -- state machine -----------------------------------------------------
    async def _advance_locked(
        self, row: dict[str, Any], *, wait: bool
    ) -> dict[str, Any]:
        txid = row['txid']

        if row['state'] in TERMINAL_STATES:
            return row

        if row['state'] == 'INITIATED':
            if not wait:
                return await self._abort_without_prepare(
                    row,
                    'RECOVERED_INITIATED',
                    'Coordinator recovered an initiated transaction that never prepared',
                )
            updated = await self.store.mark_preparing(txid)
            row = updated or await self.store.get(txid)
            if row is None:
                raise CoordinatorError(
                    'COORDINATOR_ERROR',
                    'Transaction record disappeared during prepare',
                )

        if row['state'] == 'PREPARING':
            if not wait:
                if self._is_young(row):
                    return row
                return await self._abort_without_prepare(
                    row,
                    'RECOVERED_PREPARING',
                    'Coordinator recovered a prepare that did not finish',
                )

            result = await self._prepare_all(row)
            if result is None:
                # An outcome could not be determined. Leave the row PREPARING;
                # recovery will wait out the grace period and abort.
                return await self.store.get(txid) or row

            vote_yes, error_code, message = result
            if not vote_yes:
                return await self._abort_after_prepare(
                    row, error_code, message
                )

            updated = await self.store.mark_prepared(txid)
            row = updated or await self.store.get(txid)
            if row is None:
                raise CoordinatorError(
                    'COORDINATOR_ERROR',
                    'Transaction record disappeared after prepare',
                )

        if row['state'] == 'PREPARED':
            if not wait:
                return await self._abort_without_prepare(
                    row,
                    'RECOVERED_UNDECIDED',
                    'Coordinator recovered a fully prepared transaction '
                    'without a durable commit decision',
                )

            if self.settings.pause_after_prepare > 0:
                logger.info(
                    'test pause after prepare  tx=%s  seconds=%s',
                    txid,
                    self.settings.pause_after_prepare,
                )
                await asyncio.sleep(self.settings.pause_after_prepare)

            updated = await self.store.record_decision(
                txid, 'COMMIT', 'COMMIT_DECIDED'
            )
            row = updated or await self.store.get(txid)
            if row is None:
                raise CoordinatorError(
                    'COORDINATOR_ERROR',
                    'Transaction record disappeared while recording COMMIT',
                )

            if self.settings.pause_after_decision > 0:
                logger.info(
                    'test pause after decision  tx=%s  seconds=%s',
                    txid,
                    self.settings.pause_after_decision,
                )
                await asyncio.sleep(self.settings.pause_after_decision)

        row = await self.store.get(txid) or row

        if row['state'] in TERMINAL_STATES:
            return row

        if row['decision'] == 'COMMIT':
            acknowledged = await self._commit_all(row)
            if acknowledged:
                try:
                    await self.store.mark_terminal(
                        txid, 'COMPLETED', 'COMMIT_DECIDED'
                    )
                except Exception:
                    logger.exception(
                        'commit applied but COMPLETED was not recorded  tx=%s',
                        txid,
                    )
            try:
                return await self.store.get(txid) or row
            except Exception:
                # The decision is durable and the commits are idempotent.
                # Returning the last known row lets the API report PENDING
                # instead of a failure that could provoke a duplicate transfer.
                logger.exception(
                    'could not read terminal state after commit  tx=%s', txid
                )
                return row

        if row['decision'] == 'ABORT':
            acknowledged = await self._rollback_all(row)
            if acknowledged:
                try:
                    await self.store.mark_terminal(
                        txid, 'ABORTED', 'ABORT_DECIDED'
                    )
                except Exception:
                    logger.exception(
                        'rollback applied but ABORTED was not recorded  tx=%s',
                        txid,
                    )
            try:
                return await self.store.get(txid) or row
            except Exception:
                logger.exception(
                    'could not read terminal state after abort  tx=%s', txid
                )
                return row

        return row

    async def _abort_without_prepare(
        self, row: dict[str, Any], code: str, message: str
    ) -> dict[str, Any]:
        updated = await self.store.record_decision(
            row['txid'], 'ABORT', 'ABORT_DECIDED', code, message
        )
        row = updated or await self.store.get(row['txid']) or row
        return await self._rollback_after_decision(row)

    async def _abort_after_prepare(
        self, row: dict[str, Any], error_code: str | None, message: str | None
    ) -> dict[str, Any]:
        try:
            updated = await self.store.record_decision(
                row['txid'],
                'ABORT',
                'ABORT_DECIDED',
                error_code or 'PREPARE_REJECTED',
                message or 'A participant rejected the prepare',
            )
        except Exception:
            # Abort is the default outcome. Releasing participant locks is
            # safe even if the decision record could not be written; recovery
            # will record the abort again when the store is back.
            logger.exception(
                'could not record ABORT decision  tx=%s; rolling back best effort',
                row['txid'],
            )
            await self._rollback_all(row)
            raise

        row = updated or await self.store.get(row['txid']) or row
        return await self._rollback_after_decision(row)

    async def _rollback_after_decision(
        self, row: dict[str, Any]
    ) -> dict[str, Any]:
        acknowledged = await self._rollback_all(row)
        if acknowledged:
            try:
                await self.store.mark_terminal(
                    row['txid'], 'ABORTED', 'ABORT_DECIDED'
                )
            except Exception:
                logger.exception(
                    'rollback applied but ABORTED was not recorded  tx=%s',
                    row['txid'],
                )
        try:
            return await self.store.get(row['txid']) or row
        except Exception:
            return row

    def _check_replay(
        self,
        row: dict[str, Any],
        from_account: str,
        to_account: str,
        amount: Decimal,
    ) -> None:
        if (
            row['from_account'] != from_account
            or row['to_account'] != to_account
            or row['amount'] != amount
        ):
            raise CoordinatorError(
                'REQUEST_ID_CONFLICT',
                'This request_id was already used for a different transfer',
                http_status=409,
            )

    def _is_young(self, row: dict[str, Any]) -> bool:
        updated = row['updated_at']
        if not isinstance(updated, datetime):
            return False
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - updated).total_seconds()
        return age < self.settings.preparing_grace

    # -- participant RPCs ---------------------------------------------------
    async def _prepare_all(
        self, row: dict[str, Any]
    ) -> tuple[bool, str | None, str | None] | None:
        participants = [
            {
                'account': row['from_account'],
                'shard': row['from_shard'],
                'direction': 'debit',
            },
            {
                'account': row['to_account'],
                'shard': row['to_shard'],
                'direction': 'credit',
            },
        ]
        # A single global lock order across both databases. Concurrent
        # opposite-direction transfers would otherwise deadlock at prepare
        # time: each would hold one account and wait for the other.
        participants.sort(key=lambda item: item['account'])

        for participant in participants:
            outcome = await self._prepare_one(row, participant)
            vote = outcome['vote']
            if vote == 'NO':
                return (
                    False,
                    outcome.get('error_code') or 'PREPARE_REJECTED',
                    outcome.get('message') or 'A participant rejected the prepare',
                )
            if vote != 'YES':
                return None

        return (True, None, None)

    async def _prepare_one(
        self, row: dict[str, Any], participant: dict[str, str]
    ) -> dict[str, Any]:
        shard_id = participant['shard']
        gid = f"{row['txid']}:{shard_id}"

        outcome = await self._call(
            shard_id,
            'POST',
            '/internal/2pc/prepare/',
            {
                'txid': row['txid'],
                'account': participant['account'],
                'direction': participant['direction'],
                'amount': str(row['amount']),
            },
            trace_id=row['txid'],
        )

        if outcome.status_code == 200:
            vote = str(outcome.body.get('vote', '')).upper()
            if vote == 'YES':
                return {
                    'vote': 'YES',
                    'gid': gid,
                    'status': outcome.body.get('status'),
                }
            return {
                'vote': 'NO',
                'gid': gid,
                'error_code': outcome.error_code or 'PREPARE_REJECTED',
                'message': outcome.message or 'The participant voted NO',
            }

        if outcome.ok and 400 <= outcome.status_code < 500:
            return {
                'vote': 'NO',
                'gid': gid,
                'error_code': outcome.error_code or 'PREPARE_REJECTED',
                'message': outcome.message or 'The participant rejected the request',
            }

        # The prepare response was lost or the participant errored. The
        # participant bounds its own prepare, so a status query is definitive:
        # either it prepared or it rolled back.
        status = await self._call(
            shard_id,
            'GET',
            f"/internal/2pc/prepared/{quote(gid, safe='')}/",
            None,
            trace_id=row['txid'],
        )
        if status.status_code == 200:
            state = str(status.body.get('state', '')).upper()
            if state in {'PREPARED', 'COMMIT_PENDING'}:
                return {'vote': 'YES', 'gid': gid, 'status': state}
            if state in {'UNKNOWN', 'COMMITTED'}:
                return {
                    'vote': 'NO',
                    'gid': gid,
                    'error_code': 'SHARD_UNAVAILABLE',
                    'message': (
                        f'The prepare on {shard_id} did not complete; '
                        'the participant has no prepared transaction'
                    ),
                }

        logger.warning(
            'prepare outcome unknown  tx=%s  shard=%s  status=%s  error=%s',
            row['txid'],
            shard_id,
            status.status_code,
            status.error or outcome.error,
        )
        return {
            'vote': 'UNKNOWN',
            'gid': gid,
            'error_code': 'PREPARE_UNCERTAIN',
            'message': 'The prepare outcome could not be determined',
        }

    async def _commit_all(self, row: dict[str, Any]) -> bool:
        shards = sorted({row['from_shard'], row['to_shard']})
        results = await asyncio.gather(
            *(self._commit_one(row, shard_id) for shard_id in shards),
            return_exceptions=True,
        )
        for shard_id, result in zip(shards, results):
            if isinstance(result, BaseException):
                logger.warning(
                    'commit ack raised  tx=%s  shard=%s  error=%r',
                    row['txid'],
                    shard_id,
                    result,
                )
        return all(result is True for result in results)

    async def _rollback_all(self, row: dict[str, Any]) -> bool:
        shards = sorted({row['from_shard'], row['to_shard']})
        results = await asyncio.gather(
            *(self._rollback_one(row, shard_id) for shard_id in shards),
            return_exceptions=True,
        )
        for shard_id, result in zip(shards, results):
            if isinstance(result, BaseException):
                logger.warning(
                    'rollback ack raised  tx=%s  shard=%s  error=%r',
                    row['txid'],
                    shard_id,
                    result,
                )
        return all(result is True for result in results)

    async def _commit_one(self, row: dict[str, Any], shard_id: str) -> bool:
        gid = f"{row['txid']}:{shard_id}"
        outcome = await self._call(
            shard_id,
            'POST',
            '/internal/2pc/commit/',
            {'gid': gid},
            trace_id=row['txid'],
        )
        if (
            outcome.status_code == 200
            and str(outcome.body.get('status', '')).upper() == 'COMMITTED'
        ):
            return True

        logger.warning(
            'commit not acknowledged  tx=%s  shard=%s  status=%s  error=%s',
            row['txid'],
            shard_id,
            outcome.status_code,
            outcome.error_code or outcome.error,
        )
        return False

    async def _rollback_one(self, row: dict[str, Any], shard_id: str) -> bool:
        gid = f"{row['txid']}:{shard_id}"
        outcome = await self._call(
            shard_id,
            'POST',
            '/internal/2pc/rollback/',
            {'gid': gid},
            trace_id=row['txid'],
        )
        if (
            outcome.status_code == 200
            and str(outcome.body.get('status', '')).upper() == 'ROLLED_BACK'
        ):
            return True

        if (
            outcome.status_code == 409
            and outcome.error_code == 'ALREADY_COMMITTED'
        ):
            logger.critical(
                'protocol violation: ABORT attempted after COMMIT  tx=%s  shard=%s',
                row['txid'],
                shard_id,
            )
            return False

        logger.warning(
            'rollback not acknowledged  tx=%s  shard=%s  status=%s  error=%s',
            row['txid'],
            shard_id,
            outcome.status_code,
            outcome.error_code or outcome.error,
        )
        return False

    async def _call(
        self,
        shard_id: str,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        *,
        trace_id: str,
    ) -> RpcOutcome:
        tried: set[str] = set()
        last_error: str | None = None

        for _ in range(self.settings.rpc_attempts):
            replica = self.config.select(shard_id, exclude=frozenset(tried))
            if replica is None:
                break
            tried.add(replica.id)

            headers = {
                'accept': 'application/json',
                'x-request-id': trace_id,
            }
            started = time.perf_counter()
            try:
                response = await self.client.request(
                    method,
                    f'{replica.url}{path}',
                    json=payload,
                    headers=headers,
                    timeout=self.settings.rpc_timeout,
                )
            except httpx.HTTPError as exc:
                last_error = type(exc).__name__
                self.config.mark(replica, False)
                logger.warning(
                    'participant rpc failed  tx=%s  shard=%s  replica=%s  '
                    'method=%s  path=%s  error=%s',
                    trace_id,
                    shard_id,
                    replica.id,
                    method,
                    path,
                    last_error,
                )
                continue

            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            try:
                body = response.json()
            except ValueError:
                body = {'message': response.text[:200]}
            if not isinstance(body, dict):
                body = {'message': str(body)[:200]}

            if response.status_code >= 500:
                last_error = f'HTTP_{response.status_code}'
                logger.warning(
                    'participant rpc server error  tx=%s  shard=%s  replica=%s  '
                    'method=%s  path=%s  status=%s  latency=%sms',
                    trace_id,
                    shard_id,
                    replica.id,
                    method,
                    path,
                    response.status_code,
                    latency_ms,
                )
                continue

            logger.info(
                'participant rpc  tx=%s  shard=%s  replica=%s  method=%s  '
                'path=%s  status=%s  latency=%sms',
                trace_id,
                shard_id,
                replica.id,
                method,
                path,
                response.status_code,
                latency_ms,
            )
            return RpcOutcome(
                ok=True,
                status_code=response.status_code,
                body=body,
                replica_id=replica.id,
            )

        return RpcOutcome(
            ok=False,
            status_code=0,
            body={
                'error_code': 'SHARD_UNAVAILABLE',
                'message': f'No healthy replica of {shard_id} accepted the request',
            },
            error=last_error or 'no_healthy_replica',
        )

    # -- recovery -----------------------------------------------------------
    async def recovery_loop(self) -> None:
        while True:
            try:
                await self.recover_once()
                if self._recovery_paused:
                    logger.info('coordinator recovery resumed')
                    self._recovery_paused = False
            except asyncio.CancelledError:
                raise
            except CoordinatorError as exc:
                # A coordinator database outage is an expected failure mode.
                # Log it once instead of dumping a traceback on every sweep.
                if not self._recovery_paused:
                    logger.warning('coordinator recovery paused: %s', exc.message)
                    self._recovery_paused = True
            except Exception:
                logger.exception('coordinator recovery sweep failed')
            await asyncio.sleep(self.settings.recovery_interval)

    async def recover_once(self) -> None:
        rows = await self.store.unresolved(limit=50)
        if not rows:
            return

        logger.info('recovery sweep  unresolved=%d', len(rows))
        for row in rows:
            txid = row['txid']
            try:
                async with self.locks.hold(txid):
                    current = await self.store.get(txid)
                    if current is None or current['state'] in TERMINAL_STATES:
                        continue
                    updated = await self._advance_locked(current, wait=False)
                    if (
                        updated is not None
                        and updated['state'] in TERMINAL_STATES
                        and updated['state'] != current['state']
                    ):
                        logger.info(
                            'recovered  tx=%s  state=%s  decision=%s',
                            txid,
                            updated['state'],
                            updated['decision'],
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('recovery failed  tx=%s', txid)
