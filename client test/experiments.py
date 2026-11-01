"""
Non-interactive harness for Assignment 2 Experiments 5, 6 and 7.

Normal traffic goes through the Gateway only, exactly like the banking client.
Experiment 6 additionally uses ``docker compose`` to stop and start the gateway
(the coordinator lives in that process) and inspects the coordinator and shard
databases to prove that the durable COMMIT decision and the prepared
transactions survive the interruption.

Usage:
    python "client test/experiments.py"          # experiments 5, 6 and 7
    python "client test/experiments.py" 5 7      # skip the interruption test

Exit code is 0 only when every check passes.
"""
import argparse
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

# A1001-A2000 belong to shard-b, A0001-A1000 to shard-a.
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

CENT = Decimal("0.01")


# ============================================================
# RESULT TRACKING
# ============================================================
class Results:
    def __init__(self):
        self.total = 0
        self.failed = 0

    def check(self, name, condition, detail=""):
        self.total += 1
        if condition:
            print(f"  PASS {name}")
        else:
            self.failed += 1
            suffix = f"  ({detail})" if detail else ""
            print(f"  FAIL {name}{suffix}")
        return bool(condition)

    @property
    def ok(self):
        return self.failed == 0


def print_header(title):
    print(f"\n{'=' * 68}\n  {title}\n{'=' * 68}")


# ============================================================
# HTTP HELPERS (gateway only)
# ============================================================
def require_balance(account):
    response = requests.get(f"{BASE_ROOT}/api/accounts/{account}/", timeout=15)
    if response.status_code != 200:
        raise RuntimeError(
            f"cannot read balance for {account}: "
            f"HTTP {response.status_code} {response.text[:160]}"
        )
    return Decimal(str(response.json()["balance"]))


def post_transfer(from_account, to_account, amount, request_id=None, timeout=180):
    payload = {
        "from_account": from_account,
        "to_account": to_account,
        "amount": str(amount),
    }
    if request_id:
        payload["request_id"] = request_id
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


def wait_health(timeout=120):
    """Wait for the gateway to report a healthy coordinator and shards."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{BASE_ROOT}/health", timeout=5)
            body = response.json()
            if (
                response.status_code == 200
                and body.get("status") == "ok"
                and body.get("coordinator", {}).get("db") == "ok"
            ):
                return body
        except (requests.exceptions.RequestException, ValueError):
            pass
        time.sleep(1)
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


# ============================================================
# DOCKER HELPERS (fault injection and durable-state inspection)
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
# EXPERIMENT 5: LOCAL AND CROSS-SHARD TRANSFERS
# ============================================================
def experiment_5(results):
    print_header("EXPERIMENT 5: LOCAL AND CROSS-SHARD TRANSFERS")
    print("  Local transfer: both accounts live in one shard database, so one")
    print("  database transaction is enough and no coordinator is involved.")
    print("  Cross-shard transfer: the debit and the credit live in different")
    print("  databases. The coordinator prepares both, records a durable")
    print("  decision, then commits both participants.\n")

    # --- Local transfer ---
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
    results.check(
        "E5 local transfer moves both balances",
        require_balance(LOCAL_A) == before_from - Decimal("10.00")
        and require_balance(LOCAL_B) == before_to + Decimal("10.00"),
        (require_balance(LOCAL_A), require_balance(LOCAL_B)),
    )

    response = post_transfer(LOCAL_B, LOCAL_A, "10.00")
    results.check(
        "E5 local reverse restores balances",
        response is not None
        and response.status_code == 200
        and require_balance(LOCAL_A) == before_from
        and require_balance(LOCAL_B) == before_to,
        (require_balance(LOCAL_A), require_balance(LOCAL_B)),
    )

    # --- Cross-shard transfer ---
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
    results.check(
        "E5 cross-shard transfer moves both balances",
        require_balance(CROSS_A) == before_from - Decimal("10.00")
        and require_balance(CROSS_B) == before_to + Decimal("10.00"),
        (require_balance(CROSS_A), require_balance(CROSS_B)),
    )

    if txid:
        status_body = transaction_body(txid)
        results.check(
            "E5 transaction status is COMPLETED",
            status_body.get("status") == "COMPLETED",
            status_body.get("status"),
        )
    else:
        results.check("E5 cross-shard transfer returned a txid", False)

    response = post_transfer(CROSS_B, CROSS_A, "10.00")
    results.check(
        "E5 cross-shard reverse restores balances",
        response is not None
        and response.status_code == 200
        and require_balance(CROSS_A) == before_from
        and require_balance(CROSS_B) == before_to,
        (require_balance(CROSS_A), require_balance(CROSS_B)),
    )


# ============================================================
# EXPERIMENT 6: INTERRUPTED DISTRIBUTED TRANSACTION
# ============================================================
def experiment_6(results):
    print_header("EXPERIMENT 6: INTERRUPTED DISTRIBUTED TRANSACTION")
    print("  A cross-shard transfer is paused after the durable COMMIT")
    print("  decision. The gateway (and with it the coordinator) is stopped")
    print("  before the participants acknowledge. After a restart, recovery")
    print("  must finish the recorded COMMIT exactly once.\n")

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
            "E6 gateway started with a post-decision pause",
            process.returncode == 0,
            output_tail(process),
        )
        results.check(
            "E6 gateway is healthy before the transfer",
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

        print("  stopping the gateway before the commits are acknowledged ...")
        process = compose("stop", "gateway")
        results.check(
            "E6 gateway stopped",
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

        print("  restarting the gateway with recovery enabled ...")
        process = compose(
            "up", "-d", "gateway",
            env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "0"},
        )
        results.check(
            "E6 gateway restarted",
            process.returncode == 0,
            output_tail(process),
        )
        results.check(
            "E6 gateway is healthy after restart",
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
        # Always leave the stack in its normal configuration, even when the
        # experiment fails or is interrupted.
        try:
            process = compose(
                "up", "-d", "gateway",
                env={"COORD_TEST_PAUSE_AFTER_DECISION_SECONDS": "0"},
            )
            if process.returncode == 0:
                wait_health(120)
            else:
                print(
                    "  [!] could not reset the gateway pause setting: "
                    f"{output_tail(process)}"
                )
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask errors
            print(
                "  [!] could not reset the gateway pause setting: "
                f"{type(exc).__name__}: {exc}"
            )


# ============================================================
# EXPERIMENT 7: CONCURRENT CROSS-SHARD TRANSFERS
# ============================================================
def experiment_7(results):
    print_header("EXPERIMENT 7: CONCURRENT CROSS-SHARD TRANSFERS")
    print("  Opposite-direction transfers over the same two accounts run")
    print("  concurrently. A global ascending lock order must prevent")
    print("  deadlocks, every transaction must reach a terminal state, and")
    print("  money must be conserved.\n")

    before_a = require_balance(CONCURRENT_A)
    before_b = require_balance(CONCURRENT_B)
    total_before = before_a + before_b

    pairs = [
        (CONCURRENT_A, CONCURRENT_B) if index % 2 == 0
        else (CONCURRENT_B, CONCURRENT_A)
        for index in range(60)
    ]

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

    results.check(
        "E7 all concurrent transfers return 200",
        statuses.get(200) == len(pairs),
        dict(statuses),
    )
    results.check(
        "E7 all concurrent transfers are COMPLETED",
        len(txids) == len(pairs)
        and all(body.get("state") == "COMPLETED" for body in bodies),
        Counter(body.get("state") for body in bodies),
    )

    reported = Counter(
        transaction_body(txid).get("status") for txid in txids
    ) if txids else Counter()
    results.check(
        "E7 every txid reports COMPLETED",
        txids and reported.get("COMPLETED") == len(txids),
        dict(reported),
    )

    after_a = require_balance(CONCURRENT_A)
    after_b = require_balance(CONCURRENT_B)
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

    print(f"\n  {len(pairs)} cross-shard transfers finished in {elapsed:.2f}s")

    # Same-shard concurrency stays local (no coordinator involved).
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
    local_modes = Counter(body_of(response).get("mode") for response in local_responses)
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
    results.check(
        "E7 local opposite directions net to zero",
        require_balance(LOCAL_CONCURRENT_A) == local_before_a
        and require_balance(LOCAL_CONCURRENT_B) == local_before_b,
        (
            require_balance(LOCAL_CONCURRENT_A),
            require_balance(LOCAL_CONCURRENT_B),
        ),
    )


# ============================================================
# MAIN
# ============================================================
def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run Assignment 2 transfer experiments 5, 6 and 7."
    )
    parser.add_argument(
        "experiments",
        nargs="*",
        type=int,
        choices=(5, 6, 7),
        help="experiment numbers to run (default: all of 5, 6 and 7)",
    )
    args = parser.parse_args(argv)
    selected = sorted(set(args.experiments or [5, 6, 7]))

    print(f"\n{'=' * 68}")
    print("  DISTRIBUTED BANKING PLATFORM - TRANSFER EXPERIMENTS")
    print(f"{'=' * 68}")
    print(f"  Gateway: {BASE_ROOT}")
    print(
        "  Accounts: "
        f"local={LOCAL_A}/{LOCAL_B}  "
        f"cross={CROSS_A}/{CROSS_B}  "
        f"fault={FAULT_A}/{FAULT_B}  "
        f"concurrent={CONCURRENT_A}/{CONCURRENT_B}"
    )

    if wait_health(30) is None:
        print("\n  FAIL gateway and coordinator are not healthy")
        return 1

    results = Results()
    runners = {5: experiment_5, 6: experiment_6, 7: experiment_7}
    for number in selected:
        try:
            runners[number](results)
        except Exception as exc:  # noqa: BLE001 - report instead of crashing
            results.total += 1
            results.failed += 1
            print(
                f"\n  FAIL experiment {number} aborted: "
                f"{type(exc).__name__}: {exc}"
            )

    print(f"\n{'=' * 68}")
    print("  SUMMARY")
    print(f"{'=' * 68}")
    passed = results.total - results.failed
    print(f"  {passed}/{results.total} checks passed")
    print(f"\n  RESULT: {'PASS' if results.ok else 'FAIL'}")
    return 0 if results.ok else 1


if __name__ == "__main__":
    sys.exit(main())
