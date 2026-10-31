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
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from balancer import ConfigError, GatewayConfig, load_config

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        config = load_config()
    except ConfigError:
        logger.exception('Gateway configuration is invalid; refusing to start')
        raise

    app.state.config = config
    app.state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(config.request_timeout),
        follow_redirects=False,
    )
    app.state.probe_task = asyncio.create_task(_probe_loop(app))

    logger.info(
        'gateway started  shards=%s  replicas=%s',
        sorted(config.replicas),
        [replica.id for replica in config.all_replicas()],
    )
    try:
        yield
    finally:
        app.state.probe_task.cancel()
        try:
            await app.state.probe_task
        except asyncio.CancelledError:
            pass
        await app.state.client.aclose()
        logger.info('gateway stopped')


app = FastAPI(title='Banking Gateway', version='1.0', lifespan=lifespan)


# ---------------------------------------------------------------------------
# Health monitoring
# ---------------------------------------------------------------------------
async def _probe_loop(app: FastAPI) -> None:
    """Probe every replica forever, updating the in-memory health state."""
    config: GatewayConfig = app.state.config
    client: httpx.AsyncClient = app.state.client

    while True:
        for replica in config.all_replicas():
            try:
                response = await client.get(
                    f'{replica.url}/health',
                    timeout=config.health_timeout,
                )
                ok = response.status_code == 200
            except httpx.HTTPError:
                ok = False

            was_healthy = replica.healthy
            config.mark(replica, ok)
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
        ],
    }


@app.get('/health')
async def health(request: Request) -> JSONResponse:
    config: GatewayConfig = request.app.state.config
    snapshot = config.snapshot()
    all_shards_covered = all(shard['healthy'] for shard in snapshot.values())
    return JSONResponse(
        status_code=200,
        content={
            'status': 'ok' if all_shards_covered else 'degraded',
            'service': 'gateway',
            'shards': snapshot,
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
