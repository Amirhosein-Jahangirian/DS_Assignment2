"""
Raw PostgreSQL two-phase-commit participant engine.

Django's ``transaction.atomic()`` cannot issue ``PREPARE TRANSACTION`` because
it commits on exit. This module uses a short-lived psycopg2 connection in
autocommit mode and drives the participant transaction explicitly.

The participant's durable state is split in two:

* ``pg_prepared_xacts`` is PostgreSQL's authoritative PREPARED state.
* ``ParticipantCommit`` is a small commit marker that makes repeated COMMIT
  safe and prevents a later ROLLBACK from undoing a recorded commit.

This module deliberately does not scan or resolve prepared transactions on
startup. Recovery belongs to the coordinator.
"""
from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import Any

import psycopg2
from django.conf import settings
from psycopg2 import sql

from .models import Account, ParticipantCommit
from .sharding import owns

logger = logging.getLogger(__name__)

TXID_PATTERN = re.compile(r'^[A-Za-z0-9_-]{8,128}$')
GID_MAX_LENGTH = 200
DIRECTIONS = {'debit', 'credit'}

_ACCOUNT_TABLE = sql.Identifier(Account._meta.db_table)
_COMMIT_TABLE = sql.Identifier(ParticipantCommit._meta.db_table)


class ParticipantError(Exception):
    """A participant operation failed in a way the caller can act on."""

    def __init__(self, code: str, message: str, *, http_status: int = 400):
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(message)


def gid_for(txid: str) -> str:
    """Build the shard-local GID from a coordinator transaction ID."""
    if not isinstance(txid, str) or not TXID_PATTERN.fullmatch(txid):
        raise ParticipantError(
            'INVALID_TXID',
            'txid must be 8-128 characters using letters, digits, "_" or "-"',
        )

    gid = f'{txid}:{settings.SHARD_ID}'
    if len(gid) > GID_MAX_LENGTH:
        raise ParticipantError('INVALID_GID', 'GID exceeds the PostgreSQL limit')
    return gid


def _validate_gid(gid: str) -> str:
    if not isinstance(gid, str) or not gid or len(gid) > GID_MAX_LENGTH:
        raise ParticipantError('INVALID_GID', 'Malformed GID')

    txid, separator, shard_id = gid.rpartition(':')
    if not separator or shard_id != settings.SHARD_ID:
        raise ParticipantError(
            'WRONG_SHARD',
            f'GID belongs to a different shard than {settings.SHARD_ID}',
        )
    if not TXID_PATTERN.fullmatch(txid):
        raise ParticipantError('INVALID_GID', 'Malformed GID')
    return gid


def _connect():
    db = settings.DATABASES['default']
    connection = psycopg2.connect(
        dbname=db['NAME'],
        user=db['USER'],
        password=db['PASSWORD'],
        host=db['HOST'] or None,
        port=db['PORT'] or None,
        connect_timeout=5,
    )
    connection.autocommit = True
    return connection


def _prepared_exists(cursor, gid: str) -> bool:
    cursor.execute('SELECT 1 FROM pg_prepared_xacts WHERE gid = %s', (gid,))
    return cursor.fetchone() is not None


def _marker_exists(cursor, gid: str) -> bool:
    cursor.execute(
        sql.SQL('SELECT 1 FROM {} WHERE gid = %s').format(_COMMIT_TABLE),
        (gid,),
    )
    return cursor.fetchone() is not None


def _insert_marker(cursor, gid: str) -> None:
    cursor.execute(
        sql.SQL(
            'INSERT INTO {} (gid, committed_at) VALUES (%s, now()) '
            'ON CONFLICT (gid) DO NOTHING'
        ).format(_COMMIT_TABLE),
        (gid,),
    )


def _rollback_quietly(cursor) -> None:
    try:
        cursor.execute('ROLLBACK')
    except psycopg2.Error:
        pass


def prepare(
    txid: str,
    account_number: str,
    direction: str,
    amount: Decimal,
) -> dict[str, Any]:
    """Prepare one account mutation and vote YES or NO."""
    gid = gid_for(txid)

    if direction not in DIRECTIONS:
        raise ParticipantError(
            'INVALID_DIRECTION',
            "direction must be 'debit' or 'credit'",
        )

    amount = Decimal(str(amount))
    if amount <= 0:
        raise ParticipantError('INVALID_AMOUNT', 'amount must be greater than zero')

    if not owns(account_number):
        return {
            'gid': gid,
            'vote': 'NO',
            'status': 'ABORTED',
            'error_code': 'NOT_OWNED',
            'message': f'Account {account_number} is not owned by {settings.SHARD_ID}',
        }

    delta = -amount if direction == 'debit' else amount
    connection = _connect()

    try:
        with connection.cursor() as cursor:
            if _marker_exists(cursor, gid):
                return {'gid': gid, 'vote': 'YES', 'status': 'ALREADY_COMMITTED'}
            if _prepared_exists(cursor, gid):
                return {'gid': gid, 'vote': 'YES', 'status': 'ALREADY_PREPARED'}

            try:
                cursor.execute('BEGIN')
                cursor.execute(
                    sql.SQL(
                        'SELECT balance FROM {} '
                        'WHERE account_number = %s FOR UPDATE'
                    ).format(_ACCOUNT_TABLE),
                    (account_number,),
                )
                row = cursor.fetchone()

                if row is None:
                    _rollback_quietly(cursor)
                    return {
                        'gid': gid,
                        'vote': 'NO',
                        'status': 'ABORTED',
                        'error_code': 'UNKNOWN_ACCOUNT',
                        'message': (
                            f'Account {account_number} does not exist on '
                            f'{settings.SHARD_ID}'
                        ),
                    }

                balance = row[0]
                if delta < 0 and balance < amount:
                    _rollback_quietly(cursor)
                    return {
                        'gid': gid,
                        'vote': 'NO',
                        'status': 'ABORTED',
                        'error_code': 'INSUFFICIENT_FUNDS',
                        'message': f'Account {account_number} has insufficient funds',
                    }

                cursor.execute(
                    sql.SQL(
                        'UPDATE {} SET balance = balance + %s, '
                        'updated_at = now() WHERE account_number = %s'
                    ).format(_ACCOUNT_TABLE),
                    (delta, account_number),
                )
                if cursor.rowcount != 1:
                    raise ParticipantError(
                        'UPDATE_FAILED',
                        'Failed to update the participant account',
                        http_status=500,
                    )

                cursor.execute(
                    sql.SQL('PREPARE TRANSACTION {}').format(sql.Literal(gid))
                )
            except Exception:
                _rollback_quietly(cursor)
                raise

            return {'gid': gid, 'vote': 'YES', 'status': 'PREPARED'}
    finally:
        connection.close()


def commit(gid: str) -> dict[str, Any]:
    """Commit a prepared GID; repeated calls are safe no-ops."""
    _validate_gid(gid)
    connection = _connect()

    try:
        with connection.cursor() as cursor:
            prepared = _prepared_exists(cursor, gid)
            marker = _marker_exists(cursor, gid)

            if not prepared:
                if marker:
                    return {'gid': gid, 'status': 'COMMITTED'}
                raise ParticipantError(
                    'UNKNOWN_GID',
                    'No prepared transaction or commit marker exists for this GID',
                    http_status=404,
                )

            # Record the durable commit decision before applying it. If the
            # process dies after this point, a retry sees both the marker and
            # the prepared transaction and completes the commit.
            _insert_marker(cursor, gid)

            try:
                cursor.execute(
                    sql.SQL('COMMIT PREPARED {}').format(sql.Literal(gid))
                )
            except psycopg2.Error:
                # Another retry may have completed the commit between our
                # checks. The marker makes that outcome safe to report.
                if _marker_exists(cursor, gid) and not _prepared_exists(cursor, gid):
                    return {'gid': gid, 'status': 'COMMITTED'}
                raise

            return {'gid': gid, 'status': 'COMMITTED'}
    finally:
        connection.close()


def rollback(gid: str) -> dict[str, Any]:
    """Roll back a prepared GID; repeated calls are safe no-ops."""
    _validate_gid(gid)
    connection = _connect()

    try:
        with connection.cursor() as cursor:
            if _marker_exists(cursor, gid):
                raise ParticipantError(
                    'ALREADY_COMMITTED',
                    'A durable COMMIT decision exists for this GID',
                    http_status=409,
                )

            if not _prepared_exists(cursor, gid):
                return {'gid': gid, 'status': 'ROLLED_BACK'}

            try:
                cursor.execute(
                    sql.SQL('ROLLBACK PREPARED {}').format(sql.Literal(gid))
                )
            except psycopg2.Error:
                if _marker_exists(cursor, gid):
                    raise ParticipantError(
                        'ALREADY_COMMITTED',
                        'A durable COMMIT decision exists for this GID',
                        http_status=409,
                    )
                if not _prepared_exists(cursor, gid):
                    return {'gid': gid, 'status': 'ROLLED_BACK'}
                raise

            return {'gid': gid, 'status': 'ROLLED_BACK'}
    finally:
        connection.close()


def status(gid: str) -> dict[str, Any]:
    """Report the participant-side state of a GID (internal diagnostics)."""
    _validate_gid(gid)
    connection = _connect()

    try:
        with connection.cursor() as cursor:
            prepared = _prepared_exists(cursor, gid)
            committed = _marker_exists(cursor, gid)
    finally:
        connection.close()

    if committed and prepared:
        state = 'COMMIT_PENDING'
    elif committed:
        state = 'COMMITTED'
    elif prepared:
        state = 'PREPARED'
    else:
        state = 'UNKNOWN'

    return {
        'gid': gid,
        'prepared': prepared,
        'committed': committed,
        'state': state,
    }
