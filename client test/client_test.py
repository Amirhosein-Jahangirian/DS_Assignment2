"""
Interactive runner for Assignment 2 Experiments 1-7.

The banking client stays shard-unaware: every account request and every
transfer goes through the Gateway on port 8000. Docker and psql are used only
to observe the stack (ownership ranges, replica health, prepared transactions
and the coordinator log), never to route client traffic.

Each experiment reports one final PASS/FAIL result. The internal assertions
are still evaluated; set BANK_TEST_VERBOSE=1 to print them individually.

Usage:
    python "client test/client_test.py"

Press Enter at each prompt to run the next experiment. Exit code is 0 only
when every experiment passes.
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

VERBOSE = os.environ.get("BANK_TEST_VERBOSE", "").strip().lower() in {
    "1", "true", "yes", "on",
}

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

EXPERIMENT_GOALS = {
    1: "each shard owns one account range and the Gateway resolves ownership.",
    2: "reads for one account split evenly across both replicas of its shard.",
    3: "concurrent writes through both replicas preserve every update.",
    4: "a failed replica is detected and traffic moves to the healthy replica.",
    5: "local transfers use one database; cross-shard transfers use 2PC.",
    6: "a durable COMMIT survives a Gateway stop and recovers exactly once.",
    7: "concurrent transfers complete, conserve money, and leave no 2PC state.",
}

# ============================================================
# RESULT TRACKING
# ============================================================
class Results:
    """Evaluate every assertion but report one result per experiment."""

    def __init__(self):
        self.current = None
        self.assertions = []
        self.total = 0
        self.failed = 0
        self.records = {}

    def begin(self, number):
        self.current = number
        self.assertions = []

    def check(self, name, condition, detail=None):
        passed = bool(condition)
        self.total += 1
        if not passed:
            self.failed += 1
        self.assertions.append((name, passed, detail))
        if VERBOSE:
            mark = "ok" if passed else "FAIL"
            suffix = ""
            if not passed and detail is not None:
                suffix = f"  {_detail(detail)}"
            print(f"      {mark:<4}  {name}{suffix}")
        return passed

    def finish(self, number, elapsed):
        failed = [item for item in self.assertions if not item[1]]
        ok = bool(self.assertions) and not failed
        self.records[number] = {
            "ok": ok,
            "elapsed": elapsed,
            "assertions": len(self.assertions),
            "failed": len(failed),
        }

        print(f"\n  Result: {'PASS' if ok else 'FAIL'}  ({elapsed:.1f}s)")
        if not ok:
            if not self.assertions:
                print("    No assertions were recorded.")
            for name, _, detail in failed[:6]:
                print(f"    Failed assertion: {name}")
                if detail is not None:
                    print(f"      {_detail(detail)}")
            if len(failed) > 6:
                print(f"    ... and {len(failed) - 6} more failed assertions")
        return ok

    def abort(self, number, exc):
        self.check(
            "experiment completed without errors",
            False,
            f"{type(exc).__name__}: {exc}",
        )

    @property
    def ok(self):
        return bool(self.records) and all(
            record["ok"] for record in self.records.values()
        )


def _detail(value, limit=300):
    if isinstance(value, dict):
        text = ", ".join(f"{key}={value[key]}" for key in value)
    elif isinstance(value, (tuple, list)):
        text = ", ".join(str(item) for item in value)
    else:
        text = str(value)
    return text if len(text) <= limit else text[:limit] + "..."


def print_header(title):
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


def wait(prompt="\nPress Enter to continue..."):
    try:
        input(prompt)
        print()
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


def post_transfer(from_account, to_account, amount, timeout=180, quiet=False):
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
        if not quiet:
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
    print("  Database ownership")
    for shard, (service, database), low, high in (
        ("shard-a", DATABASES["shard-a"], "A0001", "A1000"),
        ("shard-b", DATABASES["shard-b"], "A1001", "A2000"),
    ):
        try:
            summary = shard_summary(service, database, low, high)
            count, lowest, highest, outside = summary
            print(
                f"    {shard}   {count} rows   {lowest}..{highest}   "
                f"{outside} outside range"
            )
        except Exception as exc:  # noqa: BLE001 - report instead of aborting
            summary = None
            print(f"    {shard}   [!] {type(exc).__name__}: {exc}")

        results.check(
            f"{shard} stores exactly 1000 accounts",
            summary is not None and summary[0] == 1000,
            summary,
        )
        results.check(
            f"{shard} covers {low}..{high}",
            summary is not None
            and summary[1] == low
            and summary[2] == high,
            summary,
        )
        results.check(
            f"{shard} owns no out-of-range account",
            summary is not None and summary[3] == 0,
            summary,
        )

    print("\n  Gateway routing")
    routes = (
        (ROUTE_A, "shard-a"),
        (ROUTE_A_EDGE, "shard-a"),
        (ROUTE_B_EDGE, "shard-b"),
        (ROUTE_B, "shard-b"),
    )
    for account, expected in routes:
        response = get_account(account)
        routed = (
            response.headers.get("x-shard", "") if response is not None else ""
        )
        served_by = (
            response.headers.get("x-served-by", "") if response is not None else ""
        )
        status = response.status_code if response is not None else "no-response"
        extra = "" if status == 200 else f"   (HTTP {status})"
        print(f"    {account} -> {routed or '-'} / {served_by or '-'}{extra}")
        results.check(
            f"{account} is routed to {expected}",
            response is not None
            and response.status_code == 200
            and routed == expected
            and served_by.startswith(f"{expected}-"),
            f"got {routed or '-'} / {served_by or '-'}",
        )

    response = get_account(UNKNOWN_ACCOUNT)
    body = body_of(response)
    status = response.status_code if response is not None else "no-response"
    print(f"    {UNKNOWN_ACCOUNT} -> {status} {body.get('error_code', '-')}")
    results.check(
        f"unowned account {UNKNOWN_ACCOUNT} is rejected",
        response is not None
        and response.status_code == 404
        and body.get("error_code") == "UNKNOWN_ACCOUNT",
        (status, body.get("error_code", "-")),
    )

    health = health_body()
    replicas_a = healthy_replicas(health or {}, "shard-a")
    replicas_b = healthy_replicas(health or {}, "shard-b")
    print("\n  Replica health")
    print(f"    shard-a: {', '.join(replicas_a) or '-'}")
    print(f"    shard-b: {', '.join(replicas_b) or '-'}")
    results.check(
        "the Gateway reports two healthy replicas per shard",
        len(replicas_a) == 2 and len(replicas_b) == 2,
        (len(replicas_a), len(replicas_b)),
    )


# ============================================================
# EXPERIMENT 2: LOAD DISTRIBUTION ACROSS REPLICAS
# ============================================================
def experiment_2(results):
    print("  Replica selection")
    for shard, account in (("shard-a", LOAD_A), ("shard-b", LOAD_B)):
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

        split = "   ".join(
            f"{replica}={count}" for replica, count in sorted(served.items())
        )
        extra = f"   wrong-shard={wrong_shard}" if wrong_shard else ""
        print(
            f"    {shard}   {account}   {statuses.get(200, 0)}/{LOAD_READS} OK"
            f"   {split}{extra}"
        )

        results.check(
            f"{shard} reads all succeed",
            statuses.get(200) == LOAD_READS,
            dict(statuses),
        )
        results.check(
            f"{shard} reads stay on the owning shard",
            wrong_shard == 0,
            wrong_shard,
        )
        expected_replicas = {f"{shard}-1", f"{shard}-2"}
        evenly_split = (
            set(served) == expected_replicas
            and max(served.values()) - min(served.values()) <= 1
        )
        results.check(
            f"{shard} reads are split across both replicas",
            evenly_split,
            dict(served),
        )


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

    split = "   ".join(
        f"{replica}={count}" for replica, count in sorted(served.items())
    )
    print(
        f"    {shard}   {account}   {statuses.get(200, 0)}/{len(tasks)} OK"
        f"   {split}"
    )

    results.check(
        f"{shard} concurrent operations all succeed",
        statuses.get(200) == len(tasks),
        dict(statuses),
    )
    results.check(
        f"{shard} concurrent operations use both replicas",
        len(served) == 2,
        dict(served),
    )
    results.check(
        f"{shard} concurrent updates produce the serialized balance",
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
    print(
        f"              balance {before} -> {after}"
        f"   restored {restored}"
    )
    results.check(
        f"{shard} balance is restored after the experiment",
        restored == before,
        (restored, before, applied),
    )
    if restore is not None and restore.status_code != 200:
        print(f"    [!] restore on {account} returned {restore.status_code}")


def experiment_3(results):
    print("  Concurrent writes")
    run_concurrent_ops(results, "shard-a", OPS_A)
    run_concurrent_ops(results, "shard-b", OPS_B)


# ============================================================
# EXPERIMENT 4: REPLICA FAILURE AND FAILOVER
# ============================================================
def experiment_4(results):
    print("  Replica failure")
    print(f"    stopping {FAILOVER_SERVICE} ...")
    try:
        process = compose("stop", FAILOVER_SERVICE)
        if process.returncode != 0:
            print(f"    [!] stop failed: {output_tail(process)}")
        results.check(
            "replica stop command succeeded",
            process.returncode == 0,
            output_tail(process),
        )

        detected = poll_health(
            lambda body: healthy_replicas(body, FAILOVER_SHARD)
            == [HEALTHY_REPLICA],
            timeout=90,
        )
        healthy = healthy_replicas(
            detected or health_body() or {}, FAILOVER_SHARD
        )
        print(f"    shard-b healthy: {', '.join(healthy) or '-'}")
        results.check(
            "Gateway detects the failed replica",
            detected is not None,
            healthy,
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
        served_text = "   ".join(
            f"{replica}={count}" for replica, count in sorted(served.items())
        )
        print(
            f"    shard-B reads: {statuses.get(200, 0)}/{len(reads)} OK"
            f"   served by {served_text or '-'}"
        )
        results.check(
            "shard-B reads all succeed during the outage",
            statuses.get(200) == len(reads),
            dict(statuses),
        )
        results.check(
            "shard-B reads all go to the healthy replica",
            set(served) == {HEALTHY_REPLICA},
            dict(served),
        )

        other = [get_account(FAILOVER_OTHER) for _ in range(4)]
        other_ok = sum(
            1
            for response in other
            if response is not None
            and response.status_code == 200
            and response.headers.get("x-shard") == "shard-a"
        )
        print(f"    shard-A reads: {other_ok}/{len(other)} OK   unaffected")
        results.check(
            "shard-A traffic is unaffected by the shard-B outage",
            other_ok == len(other),
            f"{other_ok}/{len(other)}",
        )
    finally:
        print(f"    starting {FAILOVER_SERVICE} ...")
        try:
            process = compose("start", FAILOVER_SERVICE)
            restart_ok = process.returncode == 0
            if not restart_ok:
                print(f"    [!] start failed: {output_tail(process)}")
            recovered = poll_health(
                lambda body: set(healthy_replicas(body, FAILOVER_SHARD))
                == {FAILOVER_REPLICA, HEALTHY_REPLICA},
                timeout=120,
            )
            healthy = healthy_replicas(
                recovered or health_body() or {}, FAILOVER_SHARD
            )
            print(f"    shard-b healthy: {', '.join(healthy) or '-'}")
            results.check(
                "replica restarted and is healthy again",
                restart_ok and recovered is not None,
                healthy,
            )
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask errors
            results.check(
                "replica restarted and is healthy again",
                False,
                f"{type(exc).__name__}: {exc}",
            )


# ============================================================
# EXPERIMENT 5: LOCAL AND CROSS-SHARD TRANSFERS
# ============================================================
def experiment_5(results):
    print(f"  Local transfer   {LOCAL_A} -> {LOCAL_B}")
    before_from = require_balance(LOCAL_A)
    before_to = require_balance(LOCAL_B)
    response = post_transfer(LOCAL_A, LOCAL_B, "10.00")
    body = body_of(response)

    results.check(
        "local transfer returns 200",
        response is not None and response.status_code == 200,
        body,
    )
    results.check(
        "local transfer reports mode=local",
        body.get("mode") == "local",
        body.get("mode"),
    )
    after_from = require_balance(LOCAL_A)
    after_to = require_balance(LOCAL_B)
    print(
        f"    mode=local   {before_from} -> {after_from}"
        f" | {before_to} -> {after_to}"
    )
    results.check(
        "local transfer moves both balances",
        after_from == before_from - Decimal("10.00")
        and after_to == before_to + Decimal("10.00"),
        (after_from, after_to),
    )

    response = post_transfer(LOCAL_B, LOCAL_A, "10.00")
    restored_from = require_balance(LOCAL_A)
    restored_to = require_balance(LOCAL_B)
    print("    reverse restored balances")
    results.check(
        "local reverse restores balances",
        response is not None
        and response.status_code == 200
        and restored_from == before_from
        and restored_to == before_to,
        (restored_from, restored_to),
    )

    print(f"\n  Cross-shard transfer   {CROSS_A} -> {CROSS_B}")
    before_from = require_balance(CROSS_A)
    before_to = require_balance(CROSS_B)
    response = post_transfer(CROSS_A, CROSS_B, "10.00")
    body = body_of(response)
    txid = body.get("txid")

    results.check(
        "cross-shard transfer returns 200",
        response is not None and response.status_code == 200,
        body,
    )
    results.check(
        "cross-shard transfer reports mode=cross-shard",
        body.get("mode") == "cross-shard",
        body.get("mode"),
    )
    results.check(
        "cross-shard transfer is COMPLETED",
        body.get("state") == "COMPLETED",
        body.get("state"),
    )
    after_from = require_balance(CROSS_A)
    after_to = require_balance(CROSS_B)
    print(
        f"    mode=cross-shard   {body.get('state', '-')}   "
        f"txid={txid or '-'}"
    )
    print(f"    {before_from} -> {after_from} | {before_to} -> {after_to}")
    results.check(
        "cross-shard transfer moves both balances",
        after_from == before_from - Decimal("10.00")
        and after_to == before_to + Decimal("10.00"),
        (after_from, after_to),
    )

    if txid:
        status_body = transaction_body(txid)
        print(f"    txid status: {status_body.get('status', '-')}")
        results.check(
            "transaction status reports COMPLETED",
            status_body.get("status") == "COMPLETED",
            status_body.get("status"),
        )
    else:
        results.check("cross-shard transfer returned a txid", False)

    response = post_transfer(CROSS_B, CROSS_A, "10.00")
    restored_from = require_balance(CROSS_A)
    restored_to = require_balance(CROSS_B)
    print("    reverse restored balances")
    results.check(
        "cross-shard reverse restores balances",
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
    before_source = require_balance(FAULT_A)
    before_target = require_balance(FAULT_B)
    txid = None
    thread = None

    try:
        print("  Phase 1: stop after the durable COMMIT decision")
        process = compose(
            "up", "-d", "gateway",
            env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "30"},
        )
        results.check(
            "Gateway started with a post-decision pause",
            process.returncode == 0,
            output_tail(process),
        )
        results.check(
            "Gateway is healthy before the transfer",
            wait_health(120) is not None,
        )

        thread = threading.Thread(
            target=lambda: post_transfer(
                FAULT_A, FAULT_B, "1.00", quiet=True
            ),
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
            "durable COMMIT decision recorded",
            bool(txid),
            txid or "decision not observed",
        )

        if txid:
            prepared_a = prepared_count(FAULT_A, txid)
            prepared_b = prepared_count(FAULT_B, txid)
            source_balance = require_balance(FAULT_A)
            target_balance = require_balance(FAULT_B)
            print(
                f"    state=COMMIT_DECIDED   prepared={prepared_a + prepared_b}"
                f"   balances unchanged"
            )
            results.check(
                "both participants are prepared before COMMIT",
                prepared_a == 1 and prepared_b == 1,
                (prepared_a, prepared_b),
            )
            results.check(
                "balances are untouched before COMMIT",
                source_balance == before_source
                and target_balance == before_target,
                (source_balance, target_balance),
            )

        print("    stopping the Gateway before the commits are acknowledged ...")
        process = compose("stop", "gateway")
        results.check(
            "Gateway stopped",
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
            prepared_a = prepared_count(FAULT_A, txid)
            prepared_b = prepared_count(FAULT_B, txid)
            print(
                "    Gateway stopped; decision and prepared transactions"
                " survived"
            )
            results.check(
                "COMMIT decision survives the interruption",
                row == "COMMIT_DECIDED|COMMIT",
                row,
            )
            results.check(
                "prepared transactions survive the interruption",
                prepared_a == 1 and prepared_b == 1,
                (prepared_a, prepared_b),
            )

        print("\n  Phase 2: restart and recover")
        wait("  Press Enter to restart the Gateway and run recovery...")

        process = compose(
            "up", "-d", "gateway",
            env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "0"},
        )
        results.check(
            "Gateway restarted",
            process.returncode == 0,
            output_tail(process),
        )
        results.check(
            "Gateway is healthy after restart",
            wait_health(120) is not None,
        )

        if txid:
            final = poll_until(
                lambda: transaction_body(txid),
                lambda body: body.get("status") == "COMPLETED",
                timeout=120,
            )
            prepared_a = prepared_count(FAULT_A, txid)
            prepared_b = prepared_count(FAULT_B, txid)
            source_balance = require_balance(FAULT_A)
            target_balance = require_balance(FAULT_B)
            final_status = (
                final.get("status") if isinstance(final, dict) else "-"
            )
            print(
                f"    state={final_status or '-'}"
                f"   prepared={prepared_a + prepared_b}"
                f"   applied exactly once"
            )
            results.check(
                "recovery completed the recorded COMMIT",
                final is not None,
                final,
            )
            results.check(
                "prepared transactions are resolved",
                prepared_a == 0 and prepared_b == 0,
                (prepared_a, prepared_b),
            )
            results.check(
                "credit and debit applied exactly once",
                source_balance == before_source - Decimal("1.00")
                and target_balance == before_target + Decimal("1.00"),
                (source_balance, target_balance),
            )

            response = post_transfer(FAULT_B, FAULT_A, "1.00")
            source_balance = require_balance(FAULT_A)
            target_balance = require_balance(FAULT_B)
            print("    reverse restored balances")
            results.check(
                "reverse transfer restores balances",
                response is not None
                and response.status_code == 200
                and source_balance == before_source
                and target_balance == before_target,
                (source_balance, target_balance),
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
                    "    [!] could not reset the Gateway pause setting: "
                    f"{output_tail(process)}"
                )
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask errors
            print(
                "    [!] could not reset the Gateway pause setting: "
                f"{type(exc).__name__}: {exc}"
            )


# ============================================================
# EXPERIMENT 7: CONCURRENT CROSS-SHARD AND LOCAL TRANSFERS
# ============================================================
def experiment_7(results):
    before_a = require_balance(CONCURRENT_A)
    before_b = require_balance(CONCURRENT_B)
    total_before = before_a + before_b

    pairs = [
        (CONCURRENT_A, CONCURRENT_B) if index % 2 == 0
        else (CONCURRENT_B, CONCURRENT_A)
        for index in range(60)
    ]

    print("  Cross-shard burst")
    print(f"    running {len(pairs)} concurrent transfers ...")
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
    completed = sum(
        1 for body in bodies if body.get("state") == "COMPLETED"
    )

    print(
        f"    {len(pairs)} transfers   {statuses.get(200, 0)}/{len(pairs)}"
        f" HTTP 200   {completed}/{len(pairs)} COMPLETED   {elapsed:.2f}s"
    )
    results.check(
        "all concurrent cross-shard transfers return 200",
        statuses.get(200) == len(pairs),
        dict(statuses),
    )
    results.check(
        "all concurrent cross-shard transfers are COMPLETED",
        len(txids) == len(pairs) and completed == len(pairs),
        Counter(body.get("state") for body in bodies),
    )

    reported = (
        Counter(transaction_body(txid).get("status") for txid in txids)
        if txids
        else Counter()
    )
    print(
        f"    txid status: {reported.get('COMPLETED', 0)}/{len(txids)}"
        " COMPLETED"
    )
    results.check(
        "every txid reports COMPLETED",
        bool(txids) and reported.get("COMPLETED") == len(txids),
        dict(reported),
    )

    after_a = require_balance(CONCURRENT_A)
    after_b = require_balance(CONCURRENT_B)
    print(
        f"    balances {before_a} -> {after_a} | {before_b} -> {after_b}"
        "   money conserved"
    )
    results.check(
        "money is conserved",
        after_a + after_b == total_before,
        (after_a, after_b, total_before),
    )
    results.check(
        "opposite directions net to zero",
        after_a == before_a and after_b == before_b,
        (after_a, after_b),
    )

    health = wait_health(60) or {}
    unresolved = health.get("coordinator", {}).get("unresolved")
    prepared_a = psql(
        "db-a", "banking_a", "SELECT count(*) FROM pg_prepared_xacts;"
    )
    prepared_b = psql(
        "db-b", "banking_b", "SELECT count(*) FROM pg_prepared_xacts;"
    )
    print("\n  Coordinator and participants")
    print(
        f"    unresolved={unresolved}   prepared shard-a={prepared_a}"
        f"   prepared shard-b={prepared_b}"
    )
    results.check(
        "no unresolved coordinator rows",
        unresolved == 0,
        health.get("coordinator"),
    )
    results.check(
        "no prepared transactions remain on shard A",
        prepared_a == "0",
        prepared_a,
    )
    results.check(
        "no prepared transactions remain on shard B",
        prepared_b == "0",
        prepared_b,
    )

    print("\n  Local burst")
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
    final_local_a = require_balance(LOCAL_CONCURRENT_A)
    final_local_b = require_balance(LOCAL_CONCURRENT_B)
    print(
        f"    {len(local_pairs)} transfers   {local_statuses.get(200, 0)}"
        f"/{len(local_pairs)} HTTP 200   mode=local"
    )
    print(
        f"    balances {local_before_a} -> {final_local_a}"
        f" | {local_before_b} -> {final_local_b}   money conserved"
    )

    results.check(
        "all concurrent local transfers return 200",
        local_statuses.get(200) == len(local_pairs),
        dict(local_statuses),
    )
    results.check(
        "local transfers keep mode=local",
        local_modes.get("local") == len(local_pairs),
        dict(local_modes),
    )
    results.check(
        "local money is conserved",
        final_local_a + final_local_b == local_total,
        (final_local_a, final_local_b, local_total),
    )
    results.check(
        "local opposite directions net to zero",
        final_local_a == local_before_a
        and final_local_b == local_before_b,
        (final_local_a, final_local_b),
    )


# ============================================================
# SUMMARY AND MAIN
# ============================================================
def print_summary(results):
    print_header("SUMMARY")
    print(f"  #   {'Experiment':<45} {'Result':<6}  {'Time':>5}")
    print(f"  {'-' * 65}")
    passed = 0

    for number in range(1, 8):
        record = results.records.get(number)
        if record is None:
            status = "FAIL"
            elapsed = "-"
        else:
            status = "PASS" if record["ok"] else "FAIL"
            elapsed = f"{record['elapsed']:.1f}s"
            if record["ok"]:
                passed += 1
        print(
            f"  {number}   {EXPERIMENT_TITLES[number]:<45}"
            f" {status:<6}  {elapsed:>5}"
        )

    print(f"  {'-' * 65}")
    print(f"  Experiments: {passed}/7 passed")
    if VERBOSE:
        print(
            f"  Internal assertions: {results.total - results.failed}"
            f"/{results.total} passed"
        )
    print(f"  RESULT: {'PASS' if passed == 7 else 'FAIL'}")
    print(f"{'=' * 70}\n")


def main():
    print_header("ASSIGNMENT 2 - EXPERIMENTS 1-7")
    print(f"  Gateway: {BASE_ROOT}")
    print("  All traffic goes through the Gateway; Docker and psql are")
    print("  used only to observe the stack.")

    wait("\nPress Enter to check the Gateway and start...")

    health = wait_health(30)
    if health is None:
        print("\n  [FAIL] Gateway or coordinator is not healthy.")
        print("  Check: docker compose ps")
        return 1

    replicas_a = healthy_replicas(health, "shard-a")
    replicas_b = healthy_replicas(health, "shard-b")
    print(
        f"\n  Gateway ready: shard-a={', '.join(replicas_a)}"
        f"   shard-b={', '.join(replicas_b)}   coordinator=ok"
    )

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
    current = None

    try:
        for number, runner in runners:
            print_header(f"{number}/7  {EXPERIMENT_TITLES[number].upper()}")
            print(f"  Goal: {EXPERIMENT_GOALS[number]}")
            if number == 4:
                print(
                    f"  Note: stops {FAILOVER_SERVICE} briefly; it is"
                    " restarted automatically."
                )
            if number == 6:
                print(
                    "  Note: stops and restarts the Gateway; the stack is"
                    " restored automatically."
                )
            wait(f"\nPress Enter to run Experiment {number}/7 ...")

            results.begin(number)
            current = number
            started = time.perf_counter()
            try:
                runner(results)
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # noqa: BLE001 - report instead of crashing
                print(
                    f"\n  [!] Experiment {number} aborted: "
                    f"{type(exc).__name__}: {exc}"
                )
                results.abort(number, exc)
            results.finish(number, time.perf_counter() - started)
            current = None
    except KeyboardInterrupt:
        print("\n\n  [!] Interrupted")
        if current is not None:
            results.abort(current, KeyboardInterrupt())
            results.finish(current, 0.0)

    print_summary(results)
    return 0 if results.ok and len(results.records) == 7 else 1


if __name__ == "__main__":
    sys.exit(main())
