"""
Shard-aware gateway for the banking platform.

Clients talk only to this service. For every account-scoped request the gateway
resolves the owning shard from shard_map.yaml, picks a healthy replica for that
shard, and proxies the request. Shard and replica identities are never required
from the caller; the response still exposes X-Served-By and X-Shard for
observability.

Writes (deposit/withdraw) are never retried automatically: without idempotency
keys a retry after a timeout could apply the operation twice.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from balancer import (
    ACCOUNT_PATTERN,
    ConfigError,
    GatewayConfig,
    Replica,
    load_config,
)
from coordinator import (
    TXID_PATTERN,
    Coordinator,
    CoordinatorError,
    CoordinatorSettings,
    CoordinatorStore,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(name)s  %(message)s',
)
logger = logging.getLogger('gateway')
# The probe loop makes a health request per replica every couple of seconds;
# keep httpx's per-request INFO lines out of the gateway access log.
logging.getLogger('httpx').setLevel(logging.WARNING)

# Headers copied from the upstream response back to the client. Everything
# else (content-length, transfer-encoding, connection, ...) is recomputed by
# the gateway's own HTTP stack.
FORWARDED_RESPONSE_HEADERS = ('x-request-id', 'x-served-by', 'x-shard')

REQUEST_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]{8,128}$')
MAX_TRANSFER_AMOUNT = Decimal('9999999999999.99')


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        config = load_config()
        coordinator_settings = CoordinatorSettings.from_env()
    except ConfigError:
        logger.exception('Gateway configuration is invalid; refusing to start')
        raise

    app.state.config = config
    app.state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(config.request_timeout),
        follow_redirects=False,
    )

    store = CoordinatorStore(coordinator_settings)
    try:
        await store.connect()
    except Exception:
        logger.exception('Coordinator state store is unavailable; refusing to start')
        await app.state.client.aclose()
        raise

    app.state.store = store
    app.state.coordinator = Coordinator(
        config,
        store,
        coordinator_settings,
        app.state.client,
    )
    app.state.probe_task = asyncio.create_task(_probe_loop(app))
    app.state.recovery_task = asyncio.create_task(
        app.state.coordinator.recovery_loop()
    )

    logger.info(
        'gateway started  shards=%s  replicas=%s  coordinator_db=%s/%s',
        sorted(config.replicas),
        [replica.id for replica in config.all_replicas()],
        coordinator_settings.db_host,
        coordinator_settings.db_name,
    )
    try:
        yield
    finally:
        for task in (app.state.recovery_task, app.state.probe_task):
            task.cancel()
        await asyncio.gather(
            app.state.recovery_task,
            app.state.probe_task,
            return_exceptions=True,
        )
        await store.close()
        await app.state.client.aclose()
        logger.info('gateway stopped')


app = FastAPI(title='Banking Gateway', version='1.0', lifespan=lifespan)


# ---------------------------------------------------------------------------
# Health monitoring
# ---------------------------------------------------------------------------
async def _probe(
    replica: Replica,
    client: httpx.AsyncClient,
    timeout: float,
) -> bool:
    try:
        response = await client.get(f'{replica.url}/health', timeout=timeout)
        return response.status_code == 200
    except httpx.HTTPError:
        return False


async def _probe_loop(app: FastAPI) -> None:
    """Probe every replica forever, updating the in-memory health state."""
    config: GatewayConfig = app.state.config
    client: httpx.AsyncClient = app.state.client

    while True:
        replicas = config.all_replicas()
        # Probe concurrently so one hung replica does not delay the others.
        results = await asyncio.gather(
            *(_probe(replica, client, config.health_timeout) for replica in replicas),
            return_exceptions=True,
        )

        for replica, result in zip(replicas, results):
            was_healthy = replica.healthy
            config.mark(replica, result is True)
            if was_healthy != replica.healthy:
                logger.info(
                    'replica %s (%s) is now %s',
                    replica.id,
                    replica.shard_id,
                    'healthy' if replica.healthy else 'unhealthy',
                )

        await asyncio.sleep(config.health_interval)


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------
def _error(status_code: int, error_code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            'status': 'error',
            'error_code': error_code,
            'message': message,
        },
    )


def _upstream_response(upstream: httpx.Response) -> Response:
    headers = {
        name: upstream.headers[name]
        for name in FORWARDED_RESPONSE_HEADERS
        if name in upstream.headers
    }
    media_type = upstream.headers.get('content-type', 'application/json')
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=headers,
        media_type=media_type,
    )


# ---------------------------------------------------------------------------
# Transfer validation and helpers
# ---------------------------------------------------------------------------
async def _parse_transfer(
    request: Request,
) -> tuple[dict[str, Any] | None, JSONResponse | None]:
    try:
        body = await request.json()
    except Exception:
        return None, _error(
            400,
            'MALFORMED_REQUEST',
            'Request body must be valid JSON',
        )

    if not isinstance(body, dict):
        return None, _error(
            400,
            'INVALID_INPUT',
            'Request body must be a JSON object',
        )

    from_account = body.get('from_account')
    to_account = body.get('to_account')
    raw_amount = body.get('amount')
    request_id = body.get('request_id')

    if not isinstance(from_account, str) or not ACCOUNT_PATTERN.fullmatch(
        from_account
    ):
        return None, _error(
            400,
            'INVALID_ACCOUNT',
            'from_account must use the A#### format',
        )
    if not isinstance(to_account, str) or not ACCOUNT_PATTERN.fullmatch(to_account):
        return None, _error(
            400,
            'INVALID_ACCOUNT',
            'to_account must use the A#### format',
        )
    if from_account == to_account:
        return None, _error(
            400,
            'SAME_ACCOUNT',
            'Source and destination accounts must differ',
        )

    if isinstance(raw_amount, bool) or raw_amount is None:
        return None, _error(400, 'INVALID_AMOUNT', 'amount is required')
    try:
        amount = Decimal(str(raw_amount))
        if not amount.is_finite() or amount <= 0:
            raise InvalidOperation
        quantized = amount.quantize(Decimal('0.01'))
        if amount != quantized:
            raise InvalidOperation
        amount = quantized
    except (InvalidOperation, ValueError, TypeError):
        return None, _error(
            400,
            'INVALID_AMOUNT',
            'amount must be a positive number with at most two decimal places',
        )
    if amount > MAX_TRANSFER_AMOUNT:
        return None, _error(
            400,
            'INVALID_AMOUNT',
            'amount exceeds the supported range',
        )

    if request_id is not None:
        if not isinstance(request_id, str) or not REQUEST_ID_PATTERN.fullmatch(
            request_id
        ):
            return None, _error(
                400,
                'INVALID_REQUEST_ID',
                'request_id must be 8-128 characters using letters, digits, '
                '"_" or "-"',
            )

    return {
        'from_account': from_account,
        'to_account': to_account,
        'amount': amount,
        'request_id': request_id,
    }, None


def _transfer_error_status(error_code: str | None) -> int:
    return {
        'INSUFFICIENT_FUNDS': 422,
        'UNKNOWN_ACCOUNT': 404,
        'INVALID_ACCOUNT': 400,
        'REQUEST_ID_CONFLICT': 409,
        'LOCK_TIMEOUT': 503,
        'PREPARE_TIMEOUT': 503,
        'SHARD_UNAVAILABLE': 503,
        'COORDINATOR_UNAVAILABLE': 503,
    }.get(error_code or '', 500)


def _transfer_response(row: dict[str, Any], trace_id: str) -> JSONResponse:
    body: dict[str, Any] = {
        'txid': row['txid'],
        'mode': 'cross-shard',
        'from_account': row['from_account'],
        'to_account': row['to_account'],
        'amount': str(row['amount']),
        'state': row['state'],
        'decision': row['decision'],
        'error_code': row['error_code'],
        'message': row['message'],
    }
    headers = {'x-request-id': trace_id}

    if row['state'] == 'COMPLETED':
        body['status'] = 'success'
        return JSONResponse(body, status_code=200, headers=headers)

    if row['state'] in {'ABORTED', 'ABORT_DECIDED'}:
        body['status'] = 'error'
        return JSONResponse(
            body,
            status_code=_transfer_error_status(row['error_code']),
            headers=headers,
        )

    body['status'] = 'pending'
    body['message'] = (
        row['message']
        or 'The transaction is still being resolved; query its status by txid'
    )
    return JSONResponse(body, status_code=202, headers=headers)


async def _local_transfer(
    request: Request,
    from_account: str,
    to_account: str,
    amount: Decimal,
    trace_id: str,
) -> Response:
    config: GatewayConfig = request.app.state.config
    client: httpx.AsyncClient = request.app.state.client

    shard_id = config.shard_for(from_account)
    if shard_id is None:
        return _error(
            404,
            'UNKNOWN_ACCOUNT',
            f'Account {from_account} does not belong to a configured shard',
        )

    replica = config.select(shard_id)
    if replica is None:
        return _error(
            503,
            'NO_HEALTHY_REPLICA',
            f'No healthy replica available for the shard owning {from_account}',
        )

    client_ip = request.client.host if request.client else 'unknown'
    forwarded_for = request.headers.get('x-forwarded-for')
    forwarded_for = f'{forwarded_for}, {client_ip}' if forwarded_for else client_ip
    headers = {
        'accept': 'application/json',
        'x-request-id': trace_id,
        'x-forwarded-for': forwarded_for,
    }
    payload = {
        'from_account': from_account,
        'to_account': to_account,
        'amount': str(amount),
    }

    started = time.perf_counter()
    try:
        upstream = await client.post(
            f'{replica.url}/internal/transfers/local/',
            json=payload,
            headers=headers,
        )
    except httpx.HTTPError as exc:
        config.mark(replica, False)
        logger.warning(
            'transfer mode=local from=%s to=%s shard=%s replica=%s '
            'status=502 error=%s',
            from_account,
            to_account,
            shard_id,
            replica.id,
            type(exc).__name__,
        )
        return _error(
            502,
            'OUTCOME_UNKNOWN',
            'The local transfer may or may not have been applied; '
            'query both balances before retrying',
        )

    latency_ms = round((time.perf_counter() - started) * 1000, 2)
    try:
        body = upstream.json()
    except ValueError:
        body = None

    response_headers = {
        name: upstream.headers[name]
        for name in FORWARDED_RESPONSE_HEADERS
        if name in upstream.headers
    }
    response_headers['x-request-id'] = trace_id

    if not isinstance(body, dict):
        logger.warning(
            'transfer mode=local from=%s to=%s shard=%s replica=%s '
            'status=%s latency=%sms unreadable_body',
            from_account,
            to_account,
            shard_id,
            replica.id,
            upstream.status_code,
            latency_ms,
        )
        return _error(
            upstream.status_code if upstream.status_code >= 400 else 502,
            'UPSTREAM_ERROR',
            'The shard returned an unreadable response',
        )

    logger.info(
        'transfer mode=local from=%s to=%s shard=%s replica=%s status=%s '
        'latency=%sms',
        from_account,
        to_account,
        shard_id,
        replica.id,
        upstream.status_code,
        latency_ms,
    )
    body = {**body, 'mode': 'local'}
    return JSONResponse(
        content=body,
        status_code=upstream.status_code,
        headers=response_headers,
    )


async def _cross_shard_transfer(
    request: Request,
    from_account: str,
    to_account: str,
    amount: Decimal,
    request_id: str | None,
    from_shard: str,
    to_shard: str,
    trace_id: str,
) -> Response:
    coordinator: Coordinator = request.app.state.coordinator
    try:
        row = await coordinator.submit(
            from_account=from_account,
            to_account=to_account,
            amount=amount,
            from_shard=from_shard,
            to_shard=to_shard,
            request_id=request_id,
        )
    except CoordinatorError as exc:
        logger.warning(
            'transfer mode=cross-shard from=%s to=%s status=%s code=%s',
            from_account,
            to_account,
            exc.http_status,
            exc.code,
        )
        return _error(exc.http_status, exc.code, exc.message)
    except Exception:
        logger.exception(
            'transfer mode=cross-shard from=%s to=%s failed',
            from_account,
            to_account,
        )
        return _error(
            500,
            'COORDINATOR_ERROR',
            'The coordinator could not process the transfer',
        )

    logger.info(
        'transfer mode=cross-shard tx=%s from=%s to=%s state=%s decision=%s',
        row['txid'],
        from_account,
        to_account,
        row['state'],
        row['decision'],
    )
    return _transfer_response(row, trace_id)


# ---------------------------------------------------------------------------
# Proxy core
# ---------------------------------------------------------------------------
async def _proxy(
    request: Request,
    account_number: str,
    suffix: str,
    *,
    allow_retry: bool,
) -> Response:
    config: GatewayConfig = request.app.state.config
    client: httpx.AsyncClient = request.app.state.client

    shard_id = config.shard_for(account_number)
    if shard_id is None:
        logger.info(
            'method=%s account=%s shard=- replica=- status=404 reason=unowned',
            request.method,
            account_number,
        )
        return _error(
            404,
            'UNKNOWN_ACCOUNT',
            f'Account {account_number} does not exist',
        )

    request_id = request.headers.get('x-request-id') or f'gw-{uuid.uuid4().hex[:12]}'
    body = await request.body()
    client_ip = request.client.host if request.client else 'unknown'
    forwarded_for = request.headers.get('x-forwarded-for')
    forwarded_for = f'{forwarded_for}, {client_ip}' if forwarded_for else client_ip

    tried: set[str] = set()
    attempts = 2 if allow_retry else 1
    last_error: str | None = None

    for _ in range(attempts):
        replica = config.select(shard_id, exclude=frozenset(tried))
        if replica is None:
            break
        tried.add(replica.id)

        headers = {
            'accept': 'application/json',
            'x-request-id': request_id,
            'x-forwarded-for': forwarded_for,
        }
        if body:
            headers['content-type'] = request.headers.get(
                'content-type', 'application/json'
            )

        url = f'{replica.url}/api/accounts/{account_number}/{suffix}'
        started = time.perf_counter()
        try:
            upstream = await client.request(
                request.method,
                url,
                content=body or None,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            last_error = type(exc).__name__
            config.mark(replica, False)
            logger.warning(
                'method=%s account=%s shard=%s replica=%s status=502 error=%s',
                request.method,
                account_number,
                shard_id,
                replica.id,
                last_error,
            )
            continue

        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        logger.info(
            'method=%s account=%s shard=%s replica=%s req=%s status=%s latency=%sms',
            request.method,
            account_number,
            shard_id,
            replica.id,
            request_id,
            upstream.status_code,
            latency_ms,
        )
        return _upstream_response(upstream)

    healthy = [r for r in config.replicas.get(shard_id, []) if r.healthy]
    logger.warning(
        'method=%s account=%s shard=%s status=503 error=%s healthy=%s',
        request.method,
        account_number,
        shard_id,
        last_error or 'no_healthy_replica',
        [r.id for r in healthy],
    )
    if not healthy:
        return _error(
            503,
            'NO_HEALTHY_REPLICA',
            f'No healthy replica available for the shard owning {account_number}',
        )
    return _error(
        502,
        'UPSTREAM_UNAVAILABLE',
        f'All attempts to reach a replica for {account_number} failed',
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get('/')
async def root() -> dict:
    return {
        'service': 'banking-gateway',
        'status': 'ok',
        'endpoints': [
            'GET /health',
            'GET /api/accounts/{account}/',
            'POST /api/accounts/{account}/deposit/',
            'POST /api/accounts/{account}/withdraw/',
            'POST /api/transfers/',
            'GET /api/transactions/{txid}',
        ],
    }


@app.get('/health')
async def health(request: Request) -> JSONResponse:
    config: GatewayConfig = request.app.state.config
    snapshot = config.snapshot()
    all_shards_covered = all(shard['healthy'] for shard in snapshot.values())

    coordinator_health: dict[str, Any] = {
        'db': 'unavailable',
        'unresolved': None,
    }
    try:
        coordinator_health = await request.app.state.store.health()
    except Exception:
        logger.warning('coordinator health check failed')

    healthy = all_shards_covered and coordinator_health.get('db') == 'ok'
    return JSONResponse(
        status_code=200,
        content={
            'status': 'ok' if healthy else 'degraded',
            'service': 'gateway',
            'shards': snapshot,
            'coordinator': coordinator_health,
        },
    )


@app.get('/api/accounts/{account_number}/')
async def get_balance(account_number: str, request: Request) -> Response:
    return await _proxy(request, account_number, '', allow_retry=True)


@app.post('/api/accounts/{account_number}/deposit/')
async def deposit(account_number: str, request: Request) -> Response:
    return await _proxy(request, account_number, 'deposit/', allow_retry=False)


@app.post('/api/accounts/{account_number}/withdraw/')
async def withdraw(account_number: str, request: Request) -> Response:
    return await _proxy(request, account_number, 'withdraw/', allow_retry=False)


@app.post('/api/transfers/')
@app.post('/api/transfers')
async def transfer(request: Request) -> Response:
    config: GatewayConfig = request.app.state.config
    parsed, error = await _parse_transfer(request)
    if error is not None:
        return error

    assert parsed is not None
    from_account = parsed['from_account']
    to_account = parsed['to_account']
    amount: Decimal = parsed['amount']
    request_id: str | None = parsed['request_id']

    from_shard = config.shard_for(from_account)
    to_shard = config.shard_for(to_account)
    if from_shard is None or to_shard is None:
        return _error(
            404,
            'UNKNOWN_ACCOUNT',
            'Both accounts must belong to a configured shard',
        )

    trace_id = request.headers.get('x-request-id') or f'gw-{uuid.uuid4().hex[:12]}'

    if from_shard == to_shard:
        return await _local_transfer(
            request,
            from_account,
            to_account,
            amount,
            trace_id,
        )

    return await _cross_shard_transfer(
        request,
        from_account,
        to_account,
        amount,
        request_id,
        from_shard,
        to_shard,
        trace_id,
    )


@app.get('/api/transactions/{txid}')
@app.get('/api/transactions/{txid}/')
async def transaction_status(txid: str, request: Request) -> Response:
    if not TXID_PATTERN.fullmatch(txid or ''):
        return JSONResponse(
            status_code=404,
            content={
                'status': 'UNKNOWN',
                'error_code': 'UNKNOWN_TRANSACTION',
                'txid': txid,
            },
        )

    try:
        row = await request.app.state.store.get(txid)
    except Exception:
        logger.exception('transaction status query failed  tx=%s', txid)
        return _error(
            503,
            'COORDINATOR_UNAVAILABLE',
            'The coordinator state store is unavailable',
        )

    if row is None:
        return JSONResponse(
            status_code=404,
            content={
                'status': 'UNKNOWN',
                'error_code': 'UNKNOWN_TRANSACTION',
                'txid': txid,
            },
        )

    if row['state'] == 'COMPLETED':
        public_status = 'COMPLETED'
    elif row['state'] == 'ABORTED':
        public_status = 'ABORTED'
    else:
        public_status = 'PENDING'

    return JSONResponse(
        content={
            'txid': row['txid'],
            'request_id': row['request_id'],
            'status': public_status,
            'state': row['state'],
            'decision': row['decision'],
            'from_account': row['from_account'],
            'to_account': row['to_account'],
            'amount': str(row['amount']),
            'from_shard': row['from_shard'],
            'to_shard': row['to_shard'],
            'error_code': row['error_code'],
            'message': row['message'],
            'created_at': row['created_at'].isoformat(),
            'updated_at': row['updated_at'].isoformat(),
            'completed_at': (
                row['completed_at'].isoformat()
                if row['completed_at'] is not None
                else None
            ),
        },
    )


@app.api_route('/api/accounts', methods=['GET', 'POST'])
@app.api_route('/api/accounts/', methods=['GET', 'POST'])
async def account_list() -> Response:
    return _error(
        400,
        'UNSUPPORTED_OPERATION',
        'Account listing is not available through the gateway; '
        'query a specific account instead.',
    )


@app.api_route(
    '/api/{path:path}',
    methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
)
async def unsupported(path: str, request: Request) -> Response:
    return _error(
        404,
        'UNSUPPORTED_ACTION',
        f'Unsupported action: {request.method} /api/{path}',
    )
