# -*- coding: utf-8 -*-
"""Fast pytest regressions that do not depend on external AI APIs."""
import inspect
import time
import uuid

import bank_agents
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


def test_catalog_and_rates_fail_open_when_es_is_down(monkeypatch):
    monkeypatch.setattr(bank_web, "es_online", lambda *args, **kwargs: False)
    bank_web.app.config["TESTING"] = True
    client = bank_web.app.test_client()

    t0 = time.perf_counter()
    products = client.get("/products")
    product_search = client.get("/products?q=deposit")
    rates = client.get("/rates")
    elapsed = time.perf_counter() - t0

    assert products.status_code == 200
    assert product_search.status_code == 200
    assert rates.status_code == 200
    assert elapsed < 2


def test_api_ask_fails_open_when_es_is_down(monkeypatch):
    monkeypatch.setattr(bank_web, "es_online", lambda *args, **kwargs: False)
    monkeypatch.setattr(bank_web.rag_core, "client", object())
    bank_web.app.config["TESTING"] = True
    client = bank_web.app.test_client()

    res = client.post("/api/ask", json={"question": "deposit recommendation"})
    body = res.get_json()

    assert res.status_code == 200
    assert body["answer"]


def test_login_rejects_external_next_redirect():
    bank_web.app.config["TESTING"] = True
    client = bank_web.app.test_client()
    username = f"redirect_user_{uuid.uuid4().hex[:8]}"
    password = "pw1234"
    bank_db.create_user(username, password, "테스터")

    res = client.post(
        "/login?next=https://evil.example/phish",
        data={"username": username, "password": password},
        follow_redirects=False,
    )

    assert res.status_code in (301, 302)
    assert "evil.example" not in res.headers["Location"]


def test_agent_transfer_search_recovers_from_es_error(monkeypatch):
    class BrokenES:
        def options(self, **_kwargs):
            return self

        def search(self, **_kwargs):
            raise RuntimeError("es down")

    monkeypatch.setitem(bank_agents._deps, "es", BrokenES())

    assert bank_agents._search_transfers({"match_all": {}}) is None


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
