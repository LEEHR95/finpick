# -*- coding: utf-8 -*-
"""Fast pytest regressions that do not depend on external AI APIs."""
import inspect
import uuid

import bank_db
import bank_kafka
import bank_web


def test_import_does_not_start_background_services():
    assert bank_web._background_services_started is False


def test_products_search_fails_open(monkeypatch):
    def boom(_query):
        raise RuntimeError("embedding service unavailable")

    monkeypatch.setattr(bank_web, "product_search", boom)
    bank_web.app.config["TESTING"] = True
    client = bank_web.app.test_client()

    res = client.get("/products?q=이자%20높은%20적금")

    assert res.status_code == 200


def test_two_step_transfer_flow_updates_balances():
    bank_web.app.config["TESTING"] = True
    client = bank_web.app.test_client()
    password = "pw1234"
    sender = f"pytest_sender_{uuid.uuid4().hex[:8]}"
    receiver = f"pytest_receiver_{uuid.uuid4().hex[:8]}"

    bank_db.create_user(sender, password, "테스터")
    bank_db.create_user(receiver, password, "받는이")
    sender_user = bank_db.verify_user(sender, password)
    receiver_user = bank_db.verify_user(receiver, password)
    to_account = bank_db.get_accounts(receiver_user["id"])[0]["account_no"]

    assert client.post("/login", data={"username": sender, "password": password}).status_code in (302, 200)
    lookup = client.post("/transfer", data={"action": "lookup", "bank": "FinPick", "to_account": to_account})
    assert lookup.status_code == 200
    assert b'name="amount"' in lookup.data

    execute = client.post("/transfer", data={
        "action": "execute",
        "bank": "FinPick",
        "to_account": to_account,
        "amount": "150000",
        "memo": "pytest",
    }, follow_redirects=True)

    assert execute.status_code == 200
    assert bank_db.get_accounts(sender_user["id"])[0]["balance"] == 850_000
    assert bank_db.get_accounts(receiver_user["id"])[0]["balance"] == 1_150_000


def test_kafka_consumers_use_valid_request_timeout():
    for fn in (
        bank_kafka.start_transfer_request_consumer,
        bank_kafka.start_transfer_indexer,
        bank_kafka.start_consult_consumer,
    ):
        assert "request_timeout_ms=15000" in inspect.getsource(fn)
