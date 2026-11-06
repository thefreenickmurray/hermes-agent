"""Serialize concurrent `gateway ensure` / `gateway start` clients for one home.

Desktop multiplex recovery on 2026-11-05 12:24 ET spawned four `gateway run`
processes in the same millisecond: each profile's `gateway ensure` observed
`absent` before any owner published gateway.lock (that lock is acquired after
imports). This client-side lock makes the first ensure the only spawner;
waiters then discover starting/ready.

Released when the process exits, so a crashed holder cannot wedge starts.
"""
from __future__ import annotations

import contextlib
import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None
try:
    import msvcrt
except ImportError:
    msvcrt = None


def _lock_path(home: Path) -> Path:
    return home / "gateway_ensure.lock"


def _kernel_lock(handle, acquire: bool) -> None:
    if fcntl is not None:
        fcntl.flock(handle.fileno(), (fcntl.LOCK_EX | fcntl.LOCK_NB) if acquire else fcntl.LOCK_UN)
        return
    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK if acquire else msvcrt.LK_UNLCK, 1)


@contextlib.contextmanager
def exclusive_start_lock(home: Path, timeout: float = 60.0) -> Iterator[None]:
    """Exclusive advisory lock for one Hermes home's start/ensure client."""
    if fcntl is None and msvcrt is None:
        yield
        return
    path = _lock_path(Path(home))
    path.parent.mkdir(parents=True, exist_ok=True)
    if msvcrt is not None and (not path.exists() or path.stat().st_size == 0):
        try:
            path.write_text(" ", encoding="utf-8")
        except OSError:
            pass
    handle = path.open("r+" if msvcrt is not None else "a+", encoding="utf-8")
    deadline = time.monotonic() + max(1.0, float(timeout))
    try:
        while True:
            try:
                _kernel_lock(handle, True)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("gateway start lock")
                time.sleep(0.05)
        try:
            handle.seek(0)
            handle.truncate(0)
            handle.write(f"{os.getpid()}\n")
            handle.flush()
        except OSError:
            pass
        yield
    finally:
        try:
            _kernel_lock(handle, False)
        except OSError:
            pass
        handle.close()
