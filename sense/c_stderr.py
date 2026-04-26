"""
Filter C-level stdout/stderr noise (notably libmetawear's warble BLE
handshake spam — `Connected`, `Services disconvered`, `Characteristics
discovered`, `Descriptors found`, and the cosmetic
`error 177...: Operation now in progress`) through a Python logger at
DEBUG level. With this installed, normal INFO-level runs are quiet;
bumping the log level to DEBUG resurfaces the underlying lines for
diagnostics.

The trick is fd-level: we dup fds 1 and 2 to saved fds so Python's
own logging and prints keep reaching the real terminal, then dup2 a
pipe over both fds so any C-side fprintf/printf lands in the pipe.
A daemon thread reads lines from the pipe and re-emits them through
`logging`. (warble splits its output across both stdout and stderr,
so we have to capture both.)
"""
import logging
import os
import sys
import threading


def reroute_c_stderr_to_log(
    logger_name: str = "fs.warble",
    level: int = logging.DEBUG,
) -> None:
    """
    Idempotent setup. Call once after `logging.basicConfig` (or
    equivalent) and before any code that emits to fd 1/2 from C.
    """
    if reroute_c_stderr_to_log._installed:
        return
    reroute_c_stderr_to_log._installed = True

    # Preserve the real stdout/stderr behind new fds. All Python
    # output (logging, traceback, sys.stdout/stderr writers) gets
    # pointed at the saved fds so it bypasses the pipe and reaches
    # the user as normal.
    saved_err_fd = os.dup(2)
    saved_err = os.fdopen(saved_err_fd, "w", buffering=1)
    sys.stderr = saved_err

    saved_out_fd = os.dup(1)
    saved_out = os.fdopen(saved_out_fd, "w", buffering=1)
    sys.stdout = saved_out

    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler):
            h.stream = saved_err

    # Single pipe — both fd 1 and fd 2 point at its write end.
    # Anything writing directly to fd 1 or fd 2 (i.e. native code
    # that didn't go through Python's stdio abstractions) lands here.
    r_fd, w_fd = os.pipe()
    os.dup2(w_fd, 1)
    os.dup2(w_fd, 2)
    os.close(w_fd)

    warble = logging.getLogger(logger_name)

    def reader() -> None:
        with os.fdopen(r_fd, "r") as f:
            for line in f:
                line = line.rstrip()
                if line:
                    warble.log(level, "%s", line)

    threading.Thread(
        target=reader, daemon=True, name="c-stderr-reader"
    ).start()


reroute_c_stderr_to_log._installed = False
