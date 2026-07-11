# -*- coding: utf-8 -*-
"""WSGI entrypoint for production-style imports.

Set BANK_START_WORKERS=1 only for the single process that should own Kafka
consumers and transfer indexing workers.
"""
import os

from bank_web import app, start_background_services


if os.environ.get("BANK_START_WORKERS") == "1":
    start_background_services()


application = app
