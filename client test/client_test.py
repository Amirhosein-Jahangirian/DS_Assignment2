"""
Interactive runner for Assignment 2 Experiments 1-7.

The banking client stays shard-unaware: every account request and every
transfer goes through the Gateway on port 8000. Docker and psql are used only
to observe the stack (ownership ranges, replica health, prepared transactions
and the coordinator log), never to route client traffic.

Experiments
    1. Data distribution and request routing
    2. Load distribution across replicas
    3. Concurrent operations through both replicas
    4. Replica failure and failover
    5. Local and cross-shard transfers
    6. Interrupted distributed transaction (2PC recovery)
    7. Concurrent cross-shard and local transfers

Usage:
    python "client test/client_test.py"

Press Enter at each prompt to run the next experiment. Exit code is 0 only
when every check passes.
"""
import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

import requests

# ============================================================
# CONFIGURATION
# ============================================================
SERVER_HOST = os.environ.get("BANK_SERVER_HOST", "127.0.0.1")
SERVER_PORT = os.environ.get("BANK_SERVER_PORT", "8000")
BASE_ROOT = f"http://{SERVER_HOST}:{SERVER_PORT}"

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_USER = os.environ.get("DB_USER", "banker")

# Experiment 1: boundaries and ordinary accounts in each shard.
ROUTE_A = os.environ.get("BANK_EXP_ROUTE_A", "A0500")
ROUTE_A_EDGE = os.environ.get("BANK_EXP_ROUTE_A_EDGE", "A1000")
ROUTE_B_EDGE = os.environ.get("BANK_EXP_ROUTE_B_EDGE", "A1001")
ROUTE_B = os.environ.get("BANK_EXP_ROUTE_B", "A1500")
UNKNOWN_ACCOUNT = os.environ.get("BANK_EXP_UNKNOWN", "A9999")

# Experiment 2: one read-only account per shard.
LOAD_A = os.environ.get("BANK_EXP_LOAD_A", "A0502")
LOAD_B = os.environ.get("BANK_EXP_LOAD_B", "A1502")
LOAD_READS = int(os.environ.get("BANK_EXP_LOAD_READS", "12"))

# Experiment 3: one write account per shard.
OPS_A = os.environ.get("BANK_EXP_OPS_A", "A0507")
OPS_B = os.environ.get("BANK_EXP_OPS_B", "A1007")
OPS_DEPOSITS = int(os.environ.get("BANK_EXP_OPS_DEPOSITS", "8"))
OPS_WITHDRAWALS = int(os.environ.get("BANK_EXP_OPS_WITHDRAWALS", "8"))
OPS_DEPOSIT_AMOUNT = Decimal("5.00")
OPS_WITHDRAW_AMOUNT = Decimal("2.00")

# Experiment 4: stop a shard-B replica and verify traffic moves to the other.
FAILOVER_SERVICE = os.environ.get("BANK_EXP_FAILOVER_SERVICE", "api-b1")
FAILOVER_SHARD = "shard-b"
FAILOVER_REPLICA = os.environ.get("BANK_EXP_FAILOVER_REPLICA", "shard-b-1")
HEALTHY_REPLICA = os.environ.get("BANK_EXP_HEALTHY_REPLICA", "shard-b-2")
FAILOVER_ACCOUNT = os.environ.get("BANK_EXP_FAILOVER_ACCOUNT", "A1503")
FAILOVER_OTHER = os.environ.get("BANK_EXP_FAILOVER_OTHER", "A0503")

# Experiments 5-7: same defaults as the non-interactive harness.
LOCAL_A = os.environ.get("BANK_EXP_LOCAL_A", "A1003")
LOCAL_B = os.environ.get("BANK_EXP_LOCAL_B", "A1004")
CROSS_A = os.environ.get("BANK_EXP_CROSS_A", "A1010")
CROSS_B = os.environ.get("BANK_EXP_CROSS_B", "A0510")
FAULT_A = os.environ.get("BANK_EXP_FAULT_A", "A1012")
FAULT_B = os.environ.get("BANK_EXP_FAULT_B", "A0512")
CONCURRENT_A = os.environ.get("BANK_EXP_CONCURRENCY_A", "A1011")
CONCURRENT_B = os.environ.get("BANK_EXP_CONCURRENCY_B", "A0511")
LOCAL_CONCURRENT_A = os.environ.get("BANK_EXP_LOCAL_CONCURRENCY_A", "A1013")
LOCAL_CONCURRENT_B = os.environ.get("BANK_EXP_LOCAL_CONCURRENCY_B", "A1014")

DATABASES = {
    "shard-a": ("db-a", "banking_a"),
    "shard-b": ("db-b", "banking_b"),
}

EXPERIMENT_TITLES = {
    1: "Data distribution and request routing",
    2: "Load distribution across replicas",
    3: "Concurrent operations through both replicas",
    4: "Replica failure and failover",
    5: "Local and cross-shard transfers",
    6: "Interrupted distributed transaction",
    7: "Concurrent cross-shard and local transfers",
}

# ============================================================
# RESULT TRACKING
# ============================================================
class Results:
    def __init__(self):
        self.total = 0
        self.failed = 0
        self.current = None
        self.records = {}

    def begin(self, number):
        self.current = number
        self.records.setdefault(number, [0, 0])

    def check(self, name, condition, detail=None):
        self.total += 1
        passed, total = self.records.setdefault(self.current, [0, 0])
        self.records[self.current] = [passed + 1, total + 1]
        if condition:
            print(f"  [PASS]  {name}")
        else:
            self.failed += 1
            suffix = f"  ({_detail(detail)})" if detail is not None else ""
            print(f"  [FAIL]  {name}{suffix}")
        return bool(condition)

    def abort(self, number, exc):
        self.begin(number)
        self.check(
            f"E{number} experiment completed without errors",
            False,
            f"{type(exc).__name__}: {exc}",
        )

    @property
    def ok(self):
        return self.total > 0 and self.failed == 0


def _detail(value, limit=300):
    if isinstance(value, dict):
        text = json.dumps(value, default=str, sort_keys=True)
    else:
        text = str(value)
    return text if len(text) <= limit else text[:limit] + "..."


def print_header(title):
    print(f"\n{'=' * 68}\n  {title}\n{'=' * 68}")


def wait():
    try:
        input("\nPress Enter to continue...")
    except EOFError:
        print()


# ============================================================
# HTTP HELPERS (gateway only)
# ============================================================
def get_account(account, timeout=15):
    try:
        return requests.get(
            f"{BASE_ROOT}/api/accounts/{account}/", timeout=timeout
        )
    except requests.exceptions.RequestException as exc:
        print(f"  [!] GET {account} failed: {type(exc).__name__}: {exc}")
        return None


def get_balance(account):
    response = get_account(account)
    if response is None or response.status_code != 200:
        return None
    try:
        return Decimal(str(response.json()["balance"]))
    except (ValueError, KeyError, TypeError):
        return None


def require_balance(account):
    balance = get_balance(account)
    if balance is None:
        raise RuntimeError(f"cannot read balance for {account}")
    return balance


def post_action(account, action, amount, timeout=60):
    payload = {"amount": str(amount)}
    try:
        return requests.post(
            f"{BASE_ROOT}/api/accounts/{account}/{action}/",
            json=payload,
            timeout=timeout,
        )
    except requests.exceptions.RequestException as exc:
        print(f"  [!] {action} {account} failed: {type(exc).__name__}: {exc}")
        return None


def post_transfer(from_account, to_account, amount, timeout=180):
    payload = {
        "from_account": from_account,
        "to_account": to_account,
        "amount": str(amount),
    }
    try:
        return requests.post(
            f"{BASE_ROOT}/api/transfers/", json=payload, timeout=timeout
        )
    except requests.exceptions.RequestException as exc:
        print(f"  [!] transfer request failed: {type(exc).__name__}: {exc}")
        return None


def body_of(response):
    if response is None:
        return {}
    try:
        return response.json()
    except ValueError:
        return {}


def transaction_body(txid):
    try:
        response = requests.get(
            f"{BASE_ROOT}/api/transactions/{txid}/", timeout=10
        )
        if response.status_code != 200:
            return {}
        return response.json()
    except requests.exceptions.RequestException:
        return {}


def health_body():
    try:
        response = requests.get(f"{BASE_ROOT}/health", timeout=5)
        return response.json()
    except (requests.exceptions.RequestException, ValueError):
        return None


def wait_health(timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = health_body()
        if (
            body
            and body.get("status") == "ok"
            and body.get("coordinator", {}).get("db") == "ok"
        ):
            return body
        time.sleep(1)
    return None


def poll_health(predicate, timeout=90, interval=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = health_body()
        if body and predicate(body):
            return body
        time.sleep(interval)
    return None


def poll_until(operation, predicate, timeout, interval=0.5):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = operation()
            if predicate(last):
                return last
        except Exception as exc:  # noqa: BLE001 - polling is best effort
            last = exc
        time.sleep(interval)
    return None


def healthy_replicas(body, shard):
    try:
        return list(body["shards"][shard]["healthy"])
    except (KeyError, TypeError):
        return []


# ============================================================
# DOCKER / PSQL HELPERS
# ============================================================
def compose(*args, env=None):
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        env=merged,
        timeout=300,
    )


def psql(service, database, sql):
    process = subprocess.run(
        [
            "docker", "compose", "exec", "-T", service,
            "psql", "-U", DB_USER, "-d", database, "-tAc", sql,
        ],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        timeout=60,
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"psql on {service} failed: {process.stderr.strip()[:300]}"
        )
    return process.stdout.strip()


def shard_summary(service, database, low, high):
    row = psql(
        service,
        database,
        "SELECT count(*), "
        "coalesce(min(account_number), '-'), "
        "coalesce(max(account_number), '-'), "
        "count(*) FILTER (WHERE account_number < "
        f"'{low}' OR account_number > '{high}') "
        "FROM transactions_account;",
    )
    parts = row.split("|")
    if len(parts) != 4:
        raise RuntimeError(f"unexpected psql output: {row!r}")
    return int(parts[0]), parts[1], parts[2], int(parts[3])


def shard_of(account):
    return "shard-a" if int(account[1:]) <= 1000 else "shard-b"


def prepared_count(account, txid):
    service, database = DATABASES[shard_of(account)]
    gid = f"{txid}:{shard_of(account)}"
    return int(
        psql(
            service,
            database,
            f"SELECT count(*) FROM pg_prepared_xacts WHERE gid='{gid}';",
        )
    )


def output_tail(process, limit=300):
    text = (process.stderr or process.stdout or "").strip().replace("\n", " ")
    return text[-limit:]


# ============================================================
# EXPERIMENT 1: DATA DISTRIBUTION AND REQUEST ROUTING
# ============================================================
def experiment_1(results):
    results.begin(1)
    print_header("EXPERIMENT 1: DATA DISTRIBUTION AND REQUEST ROUTING")
    print("  Each shard owns a contiguous account range and its own database.")
    print("  The client only knows account numbers: the Gateway resolves the")
    print("  owner and returns X-Shard and X-Served-By for observability.\n")
    wait()

    summaries = {}
    for shard, (service, database), low, high in (
        ("shard-a", DATABASES["shard-a"], "A0001", "A1000"),
        ("shard-b", DATABASES["shard-b"], "A1001", "A2000"),
    ):
        print(f"  Database behind {shard} ({database}):")
        try:
            summary = shard_summary(service, database, low, high)
            summaries[shard] = summary
            count, lowest, highest, outside = summary
            print(
                f"    accounts={count}  min={lowest}  max={highest}  "
                f"outside-range={outside}"
            )
        except Exception as exc:  # noqa: BLE001 - report instead of aborting
            summaries[shard] = None
            print(f"    [!] {type(exc).__name__}: {exc}")

        summary = summaries[shard]
        results.check(
            f"E1 {shard} stores exactly 1000 accounts",
            summary is not None and summary[0] == 1000,
            summary,
        )
        results.check(
            f"E1 {shard} covers {low}..{high}",
            summary is not None
            and summary[1] == low
            and summary[2] == high,
            summary,
        )
        results.check(
            f"E1 {shard} owns no out-of-range account",
            summary is not None and summary[3] == 0,
            summary,
        )
        print()

    print("  Routing checks through the Gateway (no shard chosen by the client):")
    routes = (
        (ROUTE_A, "shard-a"),
        (ROUTE_A_EDGE, "shard-a"),
        (ROUTE_B_EDGE, "shard-b"),
        (ROUTE_B, "shard-b"),
    )
    for account, expected in routes:
        response = get_account(account)
        body = body_of(response)
        served_by = (
            response.headers.get("x-served-by", "") if response is not None else ""
        )
        routed = response is not None and response.headers.get("x-shard", "")
        print(
            f"    {account}: status="
            f"{response.status_code if response is not None else 'no-response'} "
            f"shard={routed or '-'} served-by={served_by or '-'} "
            f"balance={body.get('balance', '-')}"
        )
        results.check(
            f"E1 {account} is routed to {expected}",
            response is not None
            and response.status_code == 200
            and routed == expected
            and served_by.startswith(f"{expected}-"),
            (routed, served_by, body),
        )

    response = get_account(UNKNOWN_ACCOUNT)
    body = body_of(response)
    results.check(
        f"E1 unowned account {UNKNOWN_ACCOUNT} is rejected",
        response is not None
        and response.status_code == 404
        and body.get("error_code") == "UNKNOWN_ACCOUNT",
        (response.status_code if response is not None else None, body),
    )

    health = health_body()
    results.check(
        "E1 the Gateway reports two healthy replicas per shard",
        health is not None
        and len(healthy_replicas(health, "shard-a")) == 2
        and len(healthy_replicas(health, "shard-b")) == 2,
        health.get("shards") if health else None,
    )


# ============================================================
# EXPERIMENT 2: LOAD DISTRIBUTION ACROSS REPLICAS
# ============================================================
def experiment_2(results):
    results.begin(2)
    print_header("EXPERIMENT 2: LOAD DISTRIBUTION ACROSS REPLICAS")
    print("  Repeated reads for one account are served by both replicas of")
    print("  the owning shard. Round-robin selection should split the reads")
    print("  evenly; the other shard must never be used.\n")
    wait()

    for shard, account in (("shard-a", LOAD_A), ("shard-b", LOAD_B)):
        print(f"  {LOAD_READS} reads of {account} (owner: {shard}):")
        statuses = Counter()
        served = Counter()
        wrong_shard = 0

        for _ in range(LOAD_READS):
            response = get_account(account)
            statuses[
                response.status_code if response is not None else "no-response"
            ] += 1
            if response is not None:
                served[response.headers.get("x-served-by", "(none)")] += 1
                if response.headers.get("x-shard") != shard:
                    wrong_shard += 1

        for replica, count in sorted(served.items()):
            print(f"    {replica}: {count}")
        print(f"    statuses: {dict(statuses)}")

        results.check(
            f"E2 {shard} reads all succeed",
            statuses.get(200) == LOAD_READS,
            dict(statuses),
        )
        results.check(
            f"E2 {shard} reads stay on the owning shard",
            wrong_shard == 0,
            f"{wrong_shard} wrong responses",
        )
        expected_replicas = {f"{shard}-1", f"{shard}-2"}
        evenly_split = (
            set(served) == expected_replicas
            and max(served.values()) - min(served.values()) <= 1
        )
        results.check(
            f"E2 {shard} reads are split across both replicas",
            evenly_split,
            dict(served),
        )
        print()


# ============================================================
# EXPERIMENT 3: CONCURRENT OPERATIONS THROUGH BOTH REPLICAS
# ============================================================
def run_concurrent_ops(results, shard, account):
    before = require_balance(account)
    tasks = (
        [("deposit", OPS_DEPOSIT_AMOUNT)] * OPS_DEPOSITS
        + [("withdraw", OPS_WITHDRAW_AMOUNT)] * OPS_WITHDRAWALS
    )
    expected_net = (
        OPS_DEPOSITS * OPS_DEPOSIT_AMOUNT
        - OPS_WITHDRAWALS * OPS_WITHDRAW_AMOUNT
    )

    print(
        f"  {len(tasks)} concurrent operations on {account}: "
        f"{OPS_DEPOSITS} deposits of {OPS_DEPOSIT_AMOUNT} and "
        f"{OPS_WITHDRAWALS} withdrawals of {OPS_WITHDRAW_AMOUNT}"
    )

    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        responses = list(
            executor.map(
                lambda task: post_action(account, task[0], task[1]),
                tasks,
            )
        )

    statuses = Counter(
        response.status_code if response is not None else "no-response"
        for response in responses
    )
    served = Counter(
        response.headers.get("x-served-by", "(none)")
        for response in responses
        if response is not None
    )
    after = require_balance(account)

    print(f"    statuses: {dict(statuses)}   replicas: {dict(served)}")
    print(f"    balance: {before} -> {after} (expected {before + expected_net})")

    results.check(
        f"E3 {shard} concurrent operations all succeed",
        statuses.get(200) == len(tasks),
        dict(statuses),
    )
    results.check(
        f"E3 {shard} concurrent operations use both replicas",
        len(served) == 2,
        dict(served),
    )
    results.check(
        f"E3 {shard} concurrent updates produce the serialized balance",
        after == before + expected_net,
        (before, after, before + expected_net),
    )

    applied = after - before
    restore = None
    if applied > 0:
        restore = post_action(account, "withdraw", f"{applied:.2f}")
    elif applied < 0:
        restore = post_action(account, "deposit", f"{-applied:.2f}")
    restored = require_balance(account)
    results.check(
        f"E3 {shard} balance is restored after the experiment",
        restored == before,
        (restored, before, applied),
    )
    if restore is not None and restore.status_code != 200:
        print(f"  [!] restore on {account} returned {restore.status_code}")
    print()


def experiment_3(results):
    results.begin(3)
    print_header("EXPERIMENT 3: CONCURRENT OPERATIONS THROUGH BOTH REPLICAS")
    print("  Deposits and withdrawals run concurrently through the Gateway.")
    print("  Both replicas of a shard process writes, and database row locks")
    print("  must serialize them without losing an update. Each balance is")
    print("  checked against the exact expected net effect.\n")
    wait()

    run_concurrent_ops(results, "shard-a", OPS_A)
    run_concurrent_ops(results, "shard-b", OPS_B)


# ============================================================
# EXPERIMENT 4: REPLICA FAILURE AND FAILOVER
# ============================================================
def experiment_4(results):
    results.begin(4)
    print_header("EXPERIMENT 4: REPLICA FAILURE AND FAILOVER")
    print(f"  One shard-B replica ({FAILOVER_REPLICA}) is stopped. The Gateway")
    print("  must mark it unhealthy and send every shard-B request to the")
    print("  remaining replica. Shard-A traffic must be unaffected. The")
    print("  replica is started again at the end, even if the test fails.\n")
    print(
        f"  ACTION: this script will stop {FAILOVER_SERVICE}, run the checks, "
        "and restart it."
    )
    wait()

    stopped = False
    try:
        process = compose("stop", FAILOVER_SERVICE)
        results.check(
            "E4 replica stop command succeeded",
            process.returncode == 0,
            output_tail(process),
        )
        stopped = process.returncode == 0
        if stopped:
            print(f"  {FAILOVER_SERVICE} stopped; waiting for health detection ...")

        detected = poll_health(
            lambda body: healthy_replicas(body, FAILOVER_SHARD)
            == [HEALTHY_REPLICA],
            timeout=90,
        )
        results.check(
            "E4 Gateway detects the failed replica",
            detected is not None,
            (
                healthy_replicas(health_body() or {}, FAILOVER_SHARD)
                if detected is None
                else detected.get("shards", {}).get(FAILOVER_SHARD)
            ),
        )

        reads = [get_account(FAILOVER_ACCOUNT) for _ in range(10)]
        served = Counter(
            response.headers.get("x-served-by", "(none)")
            for response in reads
            if response is not None
        )
        statuses = Counter(
            response.status_code if response is not None else "no-response"
            for response in reads
        )
        print(f"  10 reads of {FAILOVER_ACCOUNT}: {dict(statuses)} {dict(served)}")
        results.check(
            "E4 shard-B reads all succeed during the outage",
            statuses.get(200) == len(reads),
            dict(statuses),
        )
        results.check(
            "E4 shard-B reads all go to the healthy replica",
            set(served) == {HEALTHY_REPLICA},
            dict(served),
        )

        other = [get_account(FAILOVER_OTHER) for _ in range(4)]
        other_ok = all(
            response is not None
            and response.status_code == 200
            and response.headers.get("x-shard") == "shard-a"
            for response in other
        )
        other_served = Counter(
            response.headers.get("x-served-by", "(none)")
            for response in other
            if response is not None
        )
        print(f"  4 reads of {FAILOVER_OTHER}: {dict(other_served)}")
        results.check(
            "E4 shard-A traffic is unaffected by the shard-B outage",
            other_ok,
            dict(other_served),
        )
    finally:
        print(f"  starting {FAILOVER_SERVICE} again ...")
        try:
            process = compose("start", FAILOVER_SERVICE)
            restart_ok = process.returncode == 0
            if not restart_ok:
                print(f"  [!] start failed: {output_tail(process)}")
            recovered = poll_health(
                lambda body: set(healthy_replicas(body, FAILOVER_SHARD))
                == {FAILOVER_REPLICA, HEALTHY_REPLICA},
                timeout=120,
            )
            results.check(
                "E4 replica restarted and is healthy again",
                restart_ok and recovered is not None,
                (
                    healthy_replicas(health_body() or {}, FAILOVER_SHARD)
                    if recovered is None
                    else "healthy"
                ),
            )
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask errors
            results.check(
                "E4 replica restarted and is healthy again",
                False,
                f"{type(exc).__name__}: {exc}",
            )


# ============================================================
# EXPERIMENT 5: LOCAL AND CROSS-SHARD TRANSFERS
# ============================================================
def experiment_5(results):
    results.begin(5)
    print_header("EXPERIMENT 5: LOCAL AND CROSS-SHARD TRANSFERS")
    print("  Local transfer: both accounts live in one shard database, so one")
    print("  database transaction is enough and no coordinator is involved.")
    print("  Cross-shard transfer: the debit and the credit live in different")
    print("  databases. The coordinator prepares both, records a durable")
    print("  decision, then commits both participants.\n")
    wait()

    print(f"  Local transfer {LOCAL_A} -> {LOCAL_B} (10.00):")
    before_from = require_balance(LOCAL_A)
    before_to = require_balance(LOCAL_B)
    response = post_transfer(LOCAL_A, LOCAL_B, "10.00")
    body = body_of(response)

    results.check(
        "E5 local transfer returns 200",
        response is not None and response.status_code == 200,
        body,
    )
    results.check(
        "E5 local transfer reports mode=local",
        body.get("mode") == "local",
        body.get("mode"),
    )
    after_from = require_balance(LOCAL_A)
    after_to = require_balance(LOCAL_B)
    print(f"    balances: {before_from}->{after_from}  {before_to}->{after_to}")
    results.check(
        "E5 local transfer moves both balances",
        after_from == before_from - Decimal("10.00")
        and after_to == before_to + Decimal("10.00"),
        (after_from, after_to),
    )

    response = post_transfer(LOCAL_B, LOCAL_A, "10.00")
    restored_from = require_balance(LOCAL_A)
    restored_to = require_balance(LOCAL_B)
    results.check(
        "E5 local reverse restores balances",
        response is not None
        and response.status_code == 200
        and restored_from == before_from
        and restored_to == before_to,
        (restored_from, restored_to),
    )

    print(f"\n  Cross-shard transfer {CROSS_A} -> {CROSS_B} (10.00):")
    before_from = require_balance(CROSS_A)
    before_to = require_balance(CROSS_B)
    response = post_transfer(CROSS_A, CROSS_B, "10.00")
    body = body_of(response)
    txid = body.get("txid")

    results.check(
        "E5 cross-shard transfer returns 200",
        response is not None and response.status_code == 200,
        body,
    )
    results.check(
        "E5 cross-shard transfer reports mode=cross-shard",
        body.get("mode") == "cross-shard",
        body.get("mode"),
    )
    results.check(
        "E5 cross-shard transfer is COMPLETED",
        body.get("state") == "COMPLETED",
        body.get("state"),
    )
    after_from = require_balance(CROSS_A)
    after_to = require_balance(CROSS_B)
    print(f"    balances: {before_from}->{after_from}  {before_to}->{after_to}")
    results.check(
        "E5 cross-shard transfer moves both balances",
        after_from == before_from - Decimal("10.00")
        and after_to == before_to + Decimal("10.00"),
        (after_from, after_to),
    )

    if txid:
        status_body = transaction_body(txid)
        print(f"    transaction status for {txid}: {status_body.get('status')}")
        results.check(
            "E5 transaction status reports COMPLETED",
            status_body.get("status") == "COMPLETED",
            status_body.get("status"),
        )
    else:
        results.check("E5 cross-shard transfer returned a txid", False)

    response = post_transfer(CROSS_B, CROSS_A, "10.00")
    restored_from = require_balance(CROSS_A)
    restored_to = require_balance(CROSS_B)
    results.check(
        "E5 cross-shard reverse restores balances",
        response is not None
        and response.status_code == 200
        and restored_from == before_from
        and restored_to == before_to,
        (restored_from, restored_to),
    )


# ============================================================
# EXPERIMENT 6: INTERRUPTED DISTRIBUTED TRANSACTION
# ============================================================
def experiment_6(results):
    results.begin(6)
    print_header("EXPERIMENT 6: INTERRUPTED DISTRIBUTED TRANSACTION")
    print("  A cross-shard transfer is paused after the durable COMMIT")
    print("  decision. The Gateway (and with it the coordinator) is stopped")
    print("  before the participants acknowledge. After a restart, recovery")
    print("  must finish the recorded COMMIT exactly once.\n")
    print(
        "  ACTION: this script will stop and restart the Gateway. The stack "
        "is restored in a finally block."
    )
    wait()

    before_source = require_balance(FAULT_A)
    before_target = require_balance(FAULT_B)
    txid = None
    thread = None

    try:
        process = compose(
            "up", "-d", "gateway",
            env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "30"},
        )
        results.check(
            "E6 Gateway started with a post-decision pause",
            process.returncode == 0,
            output_tail(process),
        )
        results.check(
            "E6 Gateway is healthy before the transfer",
            wait_health(120) is not None,
        )

        thread = threading.Thread(
            target=lambda: post_transfer(FAULT_A, FAULT_B, "1.00"),
            daemon=True,
        )
        thread.start()

        def find_decided_transaction():
            return psql(
                "coord-db",
                "banking_coord",
                "SELECT txid FROM coordinator_transactions "
                f"WHERE from_account='{FAULT_A}' AND to_account='{FAULT_B}' "
                "AND state='COMMIT_DECIDED' "
                "ORDER BY created_at DESC LIMIT 1;",
            )

        txid = poll_until(find_decided_transaction, bool, timeout=60)
        results.check(
            "E6 durable COMMIT decision recorded",
            bool(txid),
            txid or "decision not observed",
        )

        if txid:
            results.check(
                "E6 both participants are prepared before COMMIT",
                prepared_count(FAULT_A, txid) == 1
                and prepared_count(FAULT_B, txid) == 1,
                (prepared_count(FAULT_A, txid), prepared_count(FAULT_B, txid)),
            )
            results.check(
                "E6 balances are untouched before COMMIT",
                require_balance(FAULT_A) == before_source
                and require_balance(FAULT_B) == before_target,
                (require_balance(FAULT_A), require_balance(FAULT_B)),
            )

        print("  stopping the Gateway before the commits are acknowledged ...")
        process = compose("stop", "gateway")
        results.check(
            "E6 Gateway stopped",
            process.returncode == 0,
            output_tail(process),
        )
        if thread is not None:
            thread.join(timeout=10)

        if txid:
            row = psql(
                "coord-db",
                "banking_coord",
                "SELECT state || '|' || coalesce(decision, '') "
                f"FROM coordinator_transactions WHERE txid='{txid}';",
            )
            results.check(
                "E6 COMMIT decision survives the interruption",
                row == "COMMIT_DECIDED|COMMIT",
                row,
            )
            results.check(
                "E6 prepared transactions survive the interruption",
                prepared_count(FAULT_A, txid) == 1
                and prepared_count(FAULT_B, txid) == 1,
                (prepared_count(FAULT_A, txid), prepared_count(FAULT_B, txid)),
            )

        print("\n  The stack is now interrupted; the durable decision and both")
        print("  prepared transactions are waiting in the databases.")
        wait()

        print("  restarting the Gateway with recovery enabled ...")
        process = compose(
            "up", "-d", "gateway",
            env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "0"},
        )
        results.check(
            "E6 Gateway restarted",
            process.returncode == 0,
            output_tail(process),
        )
        results.check(
            "E6 Gateway is healthy after restart",
            wait_health(120) is not None,
        )

        if txid:
            final = poll_until(
                lambda: transaction_body(txid),
                lambda body: body.get("status") == "COMPLETED",
                timeout=120,
            )
            results.check(
                "E6 recovery completed the recorded COMMIT",
                final is not None,
                final,
            )
            results.check(
                "E6 prepared transactions are resolved",
                prepared_count(FAULT_A, txid) == 0
                and prepared_count(FAULT_B, txid) == 0,
                (prepared_count(FAULT_A, txid), prepared_count(FAULT_B, txid)),
            )
            results.check(
                "E6 credit and debit applied exactly once",
                require_balance(FAULT_A) == before_source - Decimal("1.00")
                and require_balance(FAULT_B) == before_target + Decimal("1.00"),
                (require_balance(FAULT_A), require_balance(FAULT_B)),
            )

            response = post_transfer(FAULT_B, FAULT_A, "1.00")
            results.check(
                "E6 reverse transfer restores balances",
                response is not None
                and response.status_code == 200
                and require_balance(FAULT_A) == before_source
                and require_balance(FAULT_B) == before_target,
                (require_balance(FAULT_A), require_balance(FAULT_B)),
            )
    finally:
        try:
            process = compose(
                "up", "-d", "gateway",
                env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "0"},
            )
            if process.returncode == 0:
                wait_health(120)
            else:
                print(
                    "  [!] could not reset the Gateway pause setting: "
                    f"{output_tail(process)}"
                )
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask errors
            print(
                "  [!] could not reset the Gateway pause setting: "
                f"{type(exc).__name__}: {exc}"
            )


# ============================================================
# EXPERIMENT 7: CONCURRENT CROSS-SHARD AND LOCAL TRANSFERS
# ============================================================
def experiment_7(results):
    results.begin(7)
    print_header("EXPERIMENT 7: CONCURRENT CROSS-SHARD AND LOCAL TRANSFERS")
    print("  Opposite-direction transfers over the same two accounts run")
    print("  concurrently. A global ascending lock order must prevent")
    print("  deadlocks, every transaction must reach a terminal state, and")
    print("  money must be conserved. A same-shard burst follows.\n")
    wait()

    before_a = require_balance(CONCURRENT_A)
    before_b = require_balance(CONCURRENT_B)
    total_before = before_a + before_b

    pairs = [
        (CONCURRENT_A, CONCURRENT_B) if index % 2 == 0
        else (CONCURRENT_B, CONCURRENT_A)
        for index in range(60)
    ]

    print(
        f"  {len(pairs)} concurrent cross-shard transfers "
        f"(20 workers) over {CONCURRENT_A} <-> {CONCURRENT_B} ..."
    )
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=20) as executor:
        responses = list(
            executor.map(
                lambda pair: post_transfer(pair[0], pair[1], "1.00"),
                pairs,
            )
        )
    elapsed = time.perf_counter() - started

    statuses = Counter(
        response.status_code if response is not None else "no-response"
        for response in responses
    )
    bodies = [body_of(response) for response in responses]
    txids = [body.get("txid") for body in bodies if body.get("txid")]

    print(f"    statuses: {dict(statuses)}  elapsed: {elapsed:.2f}s")

    results.check(
        "E7 all concurrent cross-shard transfers return 200",
        statuses.get(200) == len(pairs),
        dict(statuses),
    )
    results.check(
        "E7 all concurrent cross-shard transfers are COMPLETED",
        len(txids) == len(pairs)
        and all(body.get("state") == "COMPLETED" for body in bodies),
        Counter(body.get("state") for body in bodies),
    )

    reported = (
        Counter(transaction_body(txid).get("status") for txid in txids)
        if txids
        else Counter()
    )
    results.check(
        "E7 every txid reports COMPLETED",
        bool(txids) and reported.get("COMPLETED") == len(txids),
        dict(reported),
    )

    after_a = require_balance(CONCURRENT_A)
    after_b = require_balance(CONCURRENT_B)
    print(f"    balances: {before_a}->{after_a}  {before_b}->{after_b}")
    results.check(
        "E7 money is conserved",
        after_a + after_b == total_before,
        (after_a, after_b, total_before),
    )
    results.check(
        "E7 opposite directions net to zero",
        after_a == before_a and after_b == before_b,
        (after_a, after_b),
    )

    health = wait_health(60) or {}
    results.check(
        "E7 no unresolved coordinator rows",
        health.get("coordinator", {}).get("unresolved") == 0,
        health.get("coordinator"),
    )
    results.check(
        "E7 no prepared transactions remain on shard A",
        psql("db-a", "banking_a", "SELECT count(*) FROM pg_prepared_xacts;") == "0",
    )
    results.check(
        "E7 no prepared transactions remain on shard B",
        psql("db-b", "banking_b", "SELECT count(*) FROM pg_prepared_xacts;") == "0",
    )

    print(
        f"\n  Concurrent local transfers "
        f"{LOCAL_CONCURRENT_A} <-> {LOCAL_CONCURRENT_B} (40 x 2.00):"
    )
    local_before_a = require_balance(LOCAL_CONCURRENT_A)
    local_before_b = require_balance(LOCAL_CONCURRENT_B)
    local_total = local_before_a + local_before_b
    local_pairs = [
        (LOCAL_CONCURRENT_A, LOCAL_CONCURRENT_B) if index % 2 == 0
        else (LOCAL_CONCURRENT_B, LOCAL_CONCURRENT_A)
        for index in range(40)
    ]
    with ThreadPoolExecutor(max_workers=15) as executor:
        local_responses = list(
            executor.map(
                lambda pair: post_transfer(pair[0], pair[1], "2.00"),
                local_pairs,
            )
        )

    local_statuses = Counter(
        response.status_code if response is not None else "no-response"
        for response in local_responses
    )
    local_modes = Counter(
        body_of(response).get("mode") for response in local_responses
    )
    print(f"    statuses: {dict(local_statuses)}  modes: {dict(local_modes)}")

    results.check(
        "E7 all concurrent local transfers return 200",
        local_statuses.get(200) == len(local_pairs),
        dict(local_statuses),
    )
    results.check(
        "E7 local transfers keep mode=local",
        local_modes.get("local") == len(local_pairs),
        dict(local_modes),
    )
    results.check(
        "E7 local money is conserved",
        require_balance(LOCAL_CONCURRENT_A)
        + require_balance(LOCAL_CONCURRENT_B)
        == local_total,
    )
    final_local_a = require_balance(LOCAL_CONCURRENT_A)
    final_local_b = require_balance(LOCAL_CONCURRENT_B)
    results.check(
        "E7 local opposite directions net to zero",
        final_local_a == local_before_a and final_local_b == local_before_b,
        (final_local_a, final_local_b),
    )


# ============================================================
# SUMMARY AND MAIN
# ============================================================
def print_summary(results):
    print_header("SUMMARY")
    for number in range(1, 8):
        passed, total = results.records.get(number, [0, 0])
        complete = total > 0 and passed == total
        mark = "[PASS]" if complete else "[FAIL]"
        print(
            f"  {mark} Experiment {number}: "
            f"{EXPERIMENT_TITLES[number]:<45} {passed}/{total}"
        )

    passed = results.total - results.failed
    print(f"\n  {passed}/{results.total} checks passed")
    print(f"  RESULT: {'PASS' if results.ok else 'FAIL'}")
    print(f"{'=' * 68}\n")


def main():
    print(f"\n{'=' * 68}")
    print("  DISTRIBUTED BANKING PLATFORM - INTERACTIVE EXPERIMENTS 1-7")
    print(f"{'=' * 68}")
    print(f"  Gateway: {BASE_ROOT}")
    print(f"  Accounts: route={ROUTE_A}/{ROUTE_B}  load={LOAD_A}/{LOAD_B}")
    print(f"            ops={OPS_A}/{OPS_B}  failover={FAILOVER_ACCOUNT}")
    print(f"            local={LOCAL_A}/{LOCAL_B}  cross={CROSS_A}/{CROSS_B}")
    print(f"            fault={FAULT_A}/{FAULT_B}")
    print(f"            concurrent={CONCURRENT_A}/{CONCURRENT_B}")

    wait()

    health = wait_health(30)
    if health is None:
        print("\n  [FAIL]  Gateway and coordinator are not healthy")
        return 1

    print("\n  Gateway health:")
    print(f"    shards: {_detail(health.get('shards', {}))}")
    print(f"    coordinator: {_detail(health.get('coordinator', {}))}")

    results = Results()
    runners = (
        (1, experiment_1),
        (2, experiment_2),
        (3, experiment_3),
        (4, experiment_4),
        (5, experiment_5),
        (6, experiment_6),
        (7, experiment_7),
    )

    try:
        for number, runner in runners:
            try:
                runner(results)
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # noqa: BLE001 - report instead of crashing
                print(
                    f"\n  [FAIL]  experiment {number} aborted: "
                    f"{type(exc).__name__}: {exc}"
                )
                results.abort(number, exc)
    except KeyboardInterrupt:
        print("\n\n  [!]  Interrupted")

    print_summary(results)
    return 0 if results.ok else 1


if __name__ == "__main__":
    sys.exit(main())
