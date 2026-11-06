from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path

from hermes_cli.gateway_start_lock import exclusive_start_lock


def test_exclusive_start_lock_serializes_two_threads():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        order: list[str] = []

        def worker(name: str) -> None:
            with exclusive_start_lock(home, timeout=5):
                order.append(f"{name}-in")
                time.sleep(0.15)
                order.append(f"{name}-out")

        a = threading.Thread(target=worker, args=("a",))
        b = threading.Thread(target=worker, args=("b",))
        a.start()
        b.start()
        a.join(timeout=5)
        b.join(timeout=5)
        assert order in (["a-in", "a-out", "b-in", "b-out"], ["b-in", "b-out", "a-in", "a-out"])
        assert (home / "gateway_ensure.lock").is_file()
