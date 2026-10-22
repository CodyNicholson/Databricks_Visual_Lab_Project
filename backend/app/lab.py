"""The DemoResult contract every demo endpoint returns, and a recorder that builds it."""

from __future__ import annotations

import time
from typing import Any, Callable


def short_error(exc: BaseException, limit: int = 600) -> str:
    """The first meaningful lines of an exception, without the JVM stack trace Spark appends."""
    text = str(exc).split("JVM stacktrace:")[0].strip()
    lines = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("at ")]
    text = "\n".join(lines[:8])
    return text if len(text) <= limit else text[: limit - 1] + "…"


class Demo:
    """Collects the commands a demo ran and the checks it verified, then renders the result.

    with Demo("hello") as d:
        d.command("tool.call('x')")
        d.check("Returned x", value == "x", f"got {value!r}")
        d.data["value"] = value
    return d.result()
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.commands: list[str] = []
        self.checks: list[dict[str, Any]] = []
        self.data: dict[str, Any] = {}
        self._started = time.perf_counter()

    def __enter__(self) -> "Demo":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def command(self, text: str) -> None:
        self.commands.append(text)

    def timed(self, text: str, fn: Callable[[], Any]) -> tuple[Any, float]:
        """Run a step, record it with its duration, and return (value, milliseconds)."""
        started = time.perf_counter()
        value = fn()
        ms = round((time.perf_counter() - started) * 1000, 1)
        self.command(f"{text}  -- {ms:g} ms")
        return value, ms

    def check(self, name: str, passed: bool, detail: Any = "") -> None:
        self.checks.append({"name": name, "passed": bool(passed), "detail": str(detail)})

    def attempt(self, command: str, fn: Callable[[], Any]) -> tuple[Any, str | None]:
        """Run a step that may fail on purpose (a pitfall demo) and record the real error it raises.

        value, error = d.attempt("client.get(missing_key)", lambda: client.get(missing_key))
        d.check("The wrong way fails with the documented error", error and "KeyError" in error, error)
        """
        self.command(command)
        try:
            return fn(), None
        except Exception as exc:  # the error is the lesson: show it, don't hide it
            error = f"{type(exc).__name__}: {short_error(exc)}"
            self.command(f"  -> {error}")
            return None, error

    def result(self) -> dict[str, Any]:
        return {
            "demo": self.name,
            "commands": self.commands,
            "data": self.data,
            "checks": self.checks,
            "elapsedMs": round((time.perf_counter() - self._started) * 1000, 2),
        }


def clamp(value: int, low: int, high: int) -> int:
    """The site is public through the tunnel: bound every size, count and duration a visitor can pick."""
    return max(low, min(high, value))
