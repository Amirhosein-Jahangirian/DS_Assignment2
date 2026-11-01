import os
import json
import uuid

import requests
import concurrent.futures

# ============================================================
# CONFIGURATION
# ============================================================
# Read from the environment so the client can target a local server,
# another host, or the gateway. The gateway is the normal entry point.
SERVER_HOST = os.environ.get("BANK_SERVER_HOST", "127.0.0.1")
SERVER_PORT = os.environ.get("BANK_SERVER_PORT", "8000")
BASE_ROOT = f"http://{SERVER_HOST}:{SERVER_PORT}"
BASE_URL = f"{BASE_ROOT}/api/accounts"
TEST_ACCOUNT = os.environ.get("BANK_TEST_ACCOUNT", "A1001")
# A1001 belongs to shard-b. These default partners only pick accounts that
# exercise both transfer paths; the client never uses ownership to route.
LOCAL_PARTNER = os.environ.get("BANK_LOCAL_ACCOUNT", "A1002")
CROSS_PARTNER = os.environ.get("BANK_CROSS_ACCOUNT", "A0501")

# Track test results
test_results = {
    "connection": False,
    "normal_ops": False,
    "error_unknown_account": False,
    "error_insufficient_funds": False,
    "error_negative_amount": False,
    "error_invalid_amount": False,
    "error_missing_fields": False,
    "error_unsupported_action": False,
    "error_malformed_request": False,
    "acid_isolation": False,
    "acid_atomicity": False,
    "acid_durability": False,
    "transfer_local": False,
    "transfer_cross_shard": False,
    "transfer_idempotency": False,
    "transfer_abort": False,

}

# ============================================================
# HELPER FUNCTIONS
# ============================================================


def print_header(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def make_request(method, endpoint, data=None, raw_data=None, verbose=True, timeout=10):
    """Make HTTP request and return response"""
    url = f"{BASE_URL}/{endpoint}"
    headers = {"Content-Type": "application/json",
               "X-Request-ID": uuid.uuid4().hex}

    try:
        if raw_data:
            response = requests.request(
                method, url, data=raw_data, headers=headers, timeout=timeout)
        else:
            response = requests.request(
                method, url, json=data, headers=headers, timeout=timeout)

        if verbose:
            print(f"  [{method}] /{endpoint} → {response.status_code}")
            try:
                resp_json = response.json()
                if 'balance' in resp_json:
                    print(f"       Balance: {resp_json['balance']}")
                elif 'error_code' in resp_json:
                    print(f"       Error: {resp_json['error_code']}")
            except:
                pass

        return response
    except requests.exceptions.RequestException as e:
        # CRITICAL: Always print the error so we know exactly why it failed
        print(f"  [!] REQUEST FAILED: {type(e).__name__} - {e}")
        return None


def wait():
    input("\nPress Enter to continue...")


def get_balance(account):
    """Return the balance for an account, or None when it cannot be read."""
    response = make_request("GET", f"{account}/", verbose=False)
    if response is None or response.status_code != 200:
        return None
    try:
        return float(response.json()["balance"])
    except (ValueError, KeyError, TypeError):
        return None


def make_transfer(from_account, to_account, amount, request_id=None, verbose=True):
    """Post a transfer through the Gateway (the only client entry point)."""
    payload = {
        "from_account": from_account,
        "to_account": to_account,
        "amount": f"{amount:.2f}" if isinstance(amount, float) else str(amount),
    }
    if request_id:
        payload["request_id"] = request_id

    url = f"{BASE_ROOT}/api/transfers/"
    headers = {
        "Content-Type": "application/json",
        "X-Request-ID": uuid.uuid4().hex,
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        if verbose:
            print(f"  [POST] /api/transfers/ → {response.status_code}")
            try:
                body = response.json()
                print(
                    f"       mode={body.get('mode')} "
                    f"state={body.get('state')} txid={body.get('txid')}"
                )
            except ValueError:
                pass
        return response
    except requests.exceptions.RequestException as e:
        print(f"  [!] REQUEST FAILED: {type(e).__name__} - {e}")
        return None


def get_transaction(txid):
    """Query the transaction-status API by transaction id."""
    try:
        return requests.get(f"{BASE_ROOT}/api/transactions/{txid}/", timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"  [!] REQUEST FAILED: {type(e).__name__} - {e}")
        return None

# ============================================================
# STAGE 0: CONNECTION TEST
# ============================================================


def stage_0():
    print_header("STAGE 0: CONNECTION TEST")
    wait()

    url = f"{BASE_ROOT}/health"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            print(f"  ✅ Gateway reachable ({url})")
            try:
                print(f"       {json.dumps(response.json())}")
            except ValueError:
                pass
            test_results["connection"] = True
        else:
            print(f"  ❌ Gateway returned {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"  [!] REQUEST FAILED: {type(e).__name__} - {e}")
        print("  ❌ Server NOT reachable")

# ============================================================
# STAGE 1: NORMAL OPERATIONS
# ============================================================


def stage_1():
    print_header("STAGE 1: NORMAL OPERATIONS")
    wait()

    print("  Initial balance:")
    make_request("GET", f"{TEST_ACCOUNT}/")

    print("\n  Deposit 100:")
    r = make_request("POST", f"{TEST_ACCOUNT}/deposit/", {"amount": 100})
    if not r or r.status_code != 200:
        print("  ❌ Deposit failed")
        return

    print("\n  Withdraw 50:")
    r = make_request("POST", f"{TEST_ACCOUNT}/withdraw/", {"amount": 50})
    if not r or r.status_code != 200:
        print("  ❌ Withdraw failed")
        return

    print("\n  Final balance:")
    make_request("GET", f"{TEST_ACCOUNT}/")
    print("  ✅ Normal operations complete")
    test_results["normal_ops"] = True

# ============================================================
# STAGE 2: ERROR HANDLING
# ============================================================
# ============================================================
# STAGE 2: ERROR HANDLING
# ============================================================


def stage_2():
    print_header("STAGE 2: ERROR HANDLING")
    wait()

    tests = [
        ("Unknown account", "POST", "Z9999/deposit/",
         {"amount": 100}, 404, "error_unknown_account"),
        ("Insufficient funds", "POST", f"{TEST_ACCOUNT}/withdraw/",
         {"amount": 99999}, 422, "error_insufficient_funds"),
        ("Negative amount", "POST", f"{TEST_ACCOUNT}/deposit/",
         {"amount": -50}, 400, "error_negative_amount"),
        ("Invalid amount", "POST", f"{TEST_ACCOUNT}/deposit/",
         {"amount": "abc"}, 400, "error_invalid_amount"),
        ("Missing fields", "POST",
         f"{TEST_ACCOUNT}/deposit/", {}, 400, "error_missing_fields"),
        ("Unsupported action", "POST", f"{TEST_ACCOUNT}/reverse/",
         {"amount": 10}, [404, 405], "error_unsupported_action"),
    ]

    for name, method, endpoint, data, expected, key in tests:
        print(f"\n  {name}:")
        r = make_request(method, endpoint, data)

        # Bulletproof status check
        status_ok = False
        if r is not None:
            if isinstance(expected, list):
                status_ok = r.status_code in expected
            else:
                status_ok = r.status_code == expected

        if status_ok:
            print(f"  ✅ {name} handled correctly")
            test_results[key] = True
        else:
            actual = r.status_code if r else "No Response"
            print(
                f"  ❌ {name} NOT handled correctly (Expected {expected}, got {actual})")

# ============================================================
# STAGE 3: MALFORMED REQUEST
# ============================================================


def stage_3():
    print_header("STAGE 3: MALFORMED REQUEST")
    wait()

    print("  Sending broken JSON:")
    r = make_request(
        "POST", f"{TEST_ACCOUNT}/deposit/", raw_data=b"{broken json")

    # Bulletproof status check
    status_ok = False
    if r is not None:
        status_ok = r.status_code == 400

    if status_ok:
        print("  ✅ Malformed request handled")
        test_results["error_malformed_request"] = True
    else:
        actual = r.status_code if r else "No Response"
        print(
            f"  ❌ Malformed request NOT handled (Expected 400, got {actual})")

# ============================================================
# STAGE 4: ACID ISOLATION
# ============================================================


def stage_4():
    print_header("STAGE 4: ACID ISOLATION (CONCURRENCY)")
    wait()

    print("  Starting balance:")
    r = make_request("GET", f"{TEST_ACCOUNT}/")
    if not r or r.status_code != 200:
        print("  ❌ Failed")
        return
    start_bal = float(r.json()['balance'])

    print("\n  Running 10 concurrent withdrawals of 10 each...")

    def withdraw():
        return make_request("POST", f"{TEST_ACCOUNT}/withdraw/", {"amount": 10}, verbose=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(withdraw) for _ in range(10)]
        concurrent.futures.wait(futures)

    print("  Final balance:")
    r = make_request("GET", f"{TEST_ACCOUNT}/")
    if not r or r.status_code != 200:
        print("  ❌ Failed")
        return
    final_bal = float(r.json()['balance'])
    expected = start_bal - 100

    if abs(final_bal - expected) < 0.01:
        print(f"  ✅ Isolation verified ({start_bal} → {final_bal})")
        test_results["acid_isolation"] = True
    else:
        print(f"  ❌ Isolation FAILED (expected {expected}, got {final_bal})")

# ============================================================
# STAGE 5: ACID ATOMICITY
# ============================================================


def stage_5():
    print_header("STAGE 5: ACID ATOMICITY")
    wait()

    print("  Balance before failed transaction:")
    r = make_request("GET", f"{TEST_ACCOUNT}/")
    if not r or r.status_code != 200:
        print("  ❌ Failed")
        return
    before = float(r.json()['balance'])

    print("\n  Attempting withdrawal larger than balance:")
    make_request("POST", f"{TEST_ACCOUNT}/withdraw/",
                 {"amount": before + 1000})

    print("\n  Balance after failed transaction:")
    r = make_request("GET", f"{TEST_ACCOUNT}/")
    if not r or r.status_code != 200:
        print("  ❌ Failed")
        return
    after = float(r.json()['balance'])

    if abs(before - after) < 0.01:
        print(f"  ✅ Atomicity verified (balance unchanged)")
        test_results["acid_atomicity"] = True
    else:
        print(f"  ❌ Atomicity FAILED (balance changed)")

# ============================================================
# STAGE 6: ACID DURABILITY
# ============================================================


def stage_6():
    print_header("STAGE 6: ACID DURABILITY")
    wait()

    print("  Depositing 500:")
    r = make_request("POST", f"{TEST_ACCOUNT}/deposit/", {"amount": 500})
    if not r or r.status_code != 200:
        print("  ❌ Failed")
        return

    print("\n  Balance before restart:")
    r = make_request("GET", f"{TEST_ACCOUNT}/")
    if not r or r.status_code != 200:
        print("  ❌ Failed")
        return
    before = float(r.json()['balance'])

    print(f"\n{'='*60}")
    print("  ACTION REQUIRED:")
    print("  1. Restart one API replica, for example:")
    print("     docker compose restart api-a1")
    print("  2. Wait a few seconds for the gateway to route to the other replica.")
    print("  3. Press Enter here to continue")
    print(f"{'='*60}")
    input()

    print("  Balance after restart:")
    r = make_request("GET", f"{TEST_ACCOUNT}/")
    if not r or r.status_code != 200:
        print("  ❌ Failed to reconnect")
        return
    after = float(r.json()['balance'])

    if abs(before - after) < 0.01:
        print(f"  ✅ Durability verified (transaction survived restart)")
        test_results["acid_durability"] = True
    else:
        print(f"  ❌ Durability FAILED (transaction lost)")

# ============================================================
# STAGE 7: TRANSFERS (LOCAL AND CROSS-SHARD)
# ============================================================


def stage_7():
    print_header("STAGE 7: TRANSFERS (LOCAL AND CROSS-SHARD)")
    wait()

    print("  Stage 7 uses only the Gateway. It never chooses a shard or replica:")
    print("  the Gateway decides whether a transfer is local or needs 2PC.")

    if get_balance(TEST_ACCOUNT) is None:
        print("  ❌ Cannot read the test account; skipping Stage 7")
        return

    # --- Local transfer: one shard, one database transaction ---
    print(f"\n  Local transfer {TEST_ACCOUNT} -> {LOCAL_PARTNER} (same shard), 10.00:")
    before_from = get_balance(TEST_ACCOUNT)
    before_to = get_balance(LOCAL_PARTNER)
    r = make_transfer(TEST_ACCOUNT, LOCAL_PARTNER, "10.00")

    local_ok = False
    if r is not None and r.status_code == 200:
        body = r.json()
        after_from = get_balance(TEST_ACCOUNT)
        after_to = get_balance(LOCAL_PARTNER)
        local_ok = (
            body.get("mode") == "local"
            and before_from is not None
            and before_to is not None
            and after_from is not None
            and after_to is not None
            and abs(after_from - (before_from - 10)) < 0.01
            and abs(after_to - (before_to + 10)) < 0.01
        )
        if local_ok:
            print(f"  ✅ Local transfer applied atomically ({before_from} → {after_from})")
        else:
            print("  ❌ Local transfer response or balances are wrong")
    else:
        print("  ❌ Local transfer failed")

    print("\n  Reversing the local transfer:")
    r = make_transfer(LOCAL_PARTNER, TEST_ACCOUNT, "10.00")
    if (
        r is not None
        and r.status_code == 200
        and before_from is not None
        and get_balance(TEST_ACCOUNT) is not None
        and abs(get_balance(TEST_ACCOUNT) - before_from) < 0.01
    ):
        print("  ✅ Local balances restored")
    else:
        print("  ❌ Could not restore local balances")
        local_ok = False
    test_results["transfer_local"] = local_ok

    # --- Cross-shard transfer: two databases, coordinated with 2PC ---
    print(f"\n  Cross-shard transfer {TEST_ACCOUNT} -> {CROSS_PARTNER}, 10.00:")
    before_from = get_balance(TEST_ACCOUNT)
    before_to = get_balance(CROSS_PARTNER)
    r = make_transfer(TEST_ACCOUNT, CROSS_PARTNER, "10.00")

    cross_ok = False
    txid = None
    if r is not None and r.status_code == 200:
        body = r.json()
        txid = body.get("txid")
        after_from = get_balance(TEST_ACCOUNT)
        after_to = get_balance(CROSS_PARTNER)
        cross_ok = (
            body.get("mode") == "cross-shard"
            and body.get("state") == "COMPLETED"
            and bool(txid)
            and before_from is not None
            and before_to is not None
            and after_from is not None
            and after_to is not None
            and abs(after_from - (before_from - 10)) < 0.01
            and abs(after_to - (before_to + 10)) < 0.01
        )
        if cross_ok:
            print(f"  ✅ Cross-shard transfer completed with 2PC (txid={txid})")
        else:
            print("  ❌ Cross-shard transfer response or balances are wrong")
    else:
        print("  ❌ Cross-shard transfer failed")

    if txid:
        print(f"\n  Querying the transaction-status API for {txid}:")
        status_response = get_transaction(txid)
        if (
            status_response is not None
            and status_response.status_code == 200
            and status_response.json().get("status") == "COMPLETED"
        ):
            print("  ✅ Status reports COMPLETED")
        else:
            print("  ❌ Status query did not report COMPLETED")
            cross_ok = False
    test_results["transfer_cross_shard"] = cross_ok

    print("\n  Reversing the cross-shard transfer:")
    r = make_transfer(CROSS_PARTNER, TEST_ACCOUNT, "10.00")
    if (
        r is not None
        and r.status_code == 200
        and before_from is not None
        and get_balance(TEST_ACCOUNT) is not None
        and abs(get_balance(TEST_ACCOUNT) - before_from) < 0.01
    ):
        print("  ✅ Cross-shard balances restored")
    else:
        print("  ❌ Could not restore cross-shard balances")
        test_results["transfer_cross_shard"] = False

    # --- Idempotent retry with a client request id ---
    print("\n  Idempotent retry with the same request_id:")
    request_id = f"stage7-{uuid.uuid4().hex[:16]}"
    before_from = get_balance(TEST_ACCOUNT)
    before_to = get_balance(CROSS_PARTNER)
    idempotent_ok = False

    r1 = make_transfer(
        TEST_ACCOUNT, CROSS_PARTNER, "5.00", request_id=request_id
    )
    if r1 is not None and r1.status_code == 200:
        txid1 = r1.json().get("txid")
        applied_from = get_balance(TEST_ACCOUNT)
        applied_to = get_balance(CROSS_PARTNER)

        print("  Retrying the identical request:")
        r2 = make_transfer(
            TEST_ACCOUNT, CROSS_PARTNER, "5.00", request_id=request_id
        )
        if r2 is not None and r2.status_code == 200:
            txid2 = r2.json().get("txid")
            repeat_from = get_balance(TEST_ACCOUNT)
            repeat_to = get_balance(CROSS_PARTNER)

            idempotent_ok = (
                bool(txid1)
                and txid1 == txid2
                and before_from is not None
                and before_to is not None
                and applied_from is not None
                and applied_to is not None
                and abs(applied_from - (before_from - 5)) < 0.01
                and abs(applied_to - (before_to + 5)) < 0.01
                and abs(repeat_from - applied_from) < 0.01
                and abs(repeat_to - applied_to) < 0.01
            )
            if idempotent_ok:
                print(f"  ✅ Duplicate returned the same txid ({txid1}) without a second debit")
            else:
                print("  ❌ Duplicate was applied twice or returned a different txid")
        else:
            print("  ❌ Duplicate request failed")

        print("  Conflicting amount with the same request_id:")
        r3 = make_transfer(
            TEST_ACCOUNT, CROSS_PARTNER, "6.00", request_id=request_id
        )
        if (
            r3 is not None
            and r3.status_code == 409
            and r3.json().get("error_code") == "REQUEST_ID_CONFLICT"
        ):
            print("  ✅ Conflicting request_id rejected with 409")
        else:
            print("  ❌ Conflicting request_id was not rejected with 409")
            idempotent_ok = False
    else:
        print("  ❌ First request with request_id failed")

    print("\n  Reversing the idempotent transfer:")
    r = make_transfer(CROSS_PARTNER, TEST_ACCOUNT, "5.00")
    if (
        r is not None
        and r.status_code == 200
        and before_from is not None
        and get_balance(TEST_ACCOUNT) is not None
        and abs(get_balance(TEST_ACCOUNT) - before_from) < 0.01
    ):
        print("  ✅ Idempotency balances restored")
    else:
        print("  ❌ Could not restore idempotency balances")
        idempotent_ok = False
    test_results["transfer_idempotency"] = idempotent_ok

    # --- Cross-shard abort: both participants must roll back ---
    print("\n  Cross-shard transfer that must abort (insufficient funds):")
    before_from = get_balance(TEST_ACCOUNT)
    before_to = get_balance(CROSS_PARTNER)
    too_much = f"{before_from + 1000:.2f}"
    r = make_transfer(TEST_ACCOUNT, CROSS_PARTNER, too_much)

    abort_ok = False
    if r is not None and r.status_code == 422:
        body = r.json()
        txid = body.get("txid")
        after_from = get_balance(TEST_ACCOUNT)
        after_to = get_balance(CROSS_PARTNER)
        abort_ok = (
            body.get("state") == "ABORTED"
            and before_from is not None
            and before_to is not None
            and after_from is not None
            and after_to is not None
            and abs(after_from - before_from) < 0.01
            and abs(after_to - before_to) < 0.01
        )
        if txid:
            status_response = get_transaction(txid)
            abort_ok = (
                abort_ok
                and status_response is not None
                and status_response.status_code == 200
                and status_response.json().get("status") == "ABORTED"
            )
        if abort_ok:
            print("  ✅ Transfer aborted on both shards; balances unchanged")
        else:
            print("  ❌ Aborted transfer changed a balance or has the wrong status")
    else:
        print("  ❌ Cross-shard abort did not return 422")
    test_results["transfer_abort"] = abort_ok


# ============================================================
# STAGE 8: SUMMARY
# ============================================================


def stage_8():
    print_header("TEST SUMMARY")

    checklist = [
        ("Connection", test_results["connection"]),
        ("Normal operations", test_results["normal_ops"]),
        ("Error: Unknown account", test_results["error_unknown_account"]),
        ("Error: Insufficient funds",
         test_results["error_insufficient_funds"]),
        ("Error: Negative amount", test_results["error_negative_amount"]),
        ("Error: Invalid amount", test_results["error_invalid_amount"]),
        ("Error: Missing fields", test_results["error_missing_fields"]),
        ("Error: Unsupported action",
         test_results["error_unsupported_action"]),
        ("Error: Malformed request", test_results["error_malformed_request"]),
        ("ACID: Isolation", test_results["acid_isolation"]),
        ("ACID: Atomicity", test_results["acid_atomicity"]),
        ("ACID: Durability", test_results["acid_durability"]),
        ("Transfer: Local", test_results["transfer_local"]),
        ("Transfer: Cross-shard", test_results["transfer_cross_shard"]),
        ("Transfer: Idempotency", test_results["transfer_idempotency"]),
        ("Transfer: Abort", test_results["transfer_abort"]),
    ]

    for name, passed in checklist:
        status = "✅" if passed else "❌"
        print(f"  {status} {name}")

    total = len(checklist)
    passed = sum(1 for _, p in checklist if p)
    print(f"\n  Result: {passed}/{total} requirements demonstrated")

    if passed == total:
        print("\n  🎉 ALL REQUIREMENTS MET!")
    else:
        print(f"\n  ⚠️  {total - passed} requirement(s) not met")

    print(f"\n{'='*60}")
    print("  VIDEO REMINDERS:")
    print("  • Show docker compose ps and the gateway health")
    print("  • Show both shard databases and their account ranges")
    print("  • Show gateway routing and replica logs")
    print("  • Show replica failover")
    print("  • Show local and cross-shard transfers, including the txid status")
    print(f"{'='*60}\n")

# ============================================================
# MAIN EXECUTION
# ============================================================


def main():
    print(f"\n{'='*60}")
    print("  DISTRIBUTED BANKING PLATFORM - A2 TEST SUITE")
    print(f"{'='*60}")
    print(f"  Server: {BASE_ROOT}")
    print(f"  Account: {TEST_ACCOUNT}")

    wait()

    try:
        stage_0()
        stage_1()
        stage_2()
        stage_3()
        stage_4()
        stage_5()
        stage_6()
        stage_7()
        stage_8()
    except KeyboardInterrupt:
        print("\n\n  ⚠️  Interrupted")
        stage_8()


if __name__ == "__main__":
    main()
