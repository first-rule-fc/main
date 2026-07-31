"""Dead-man's-switch check: has Kalshi posted any CFB game markets yet?

Read-only, no auth (see kalshi_client.py). Meant to run from the user's own
terminal via a scheduler (Task Scheduler / cron), every 8 hours, alongside
the existing healthchecks.io-based weather-app pattern.

Signal shape (per user's instruction, 2026-07-30):
- Ping the healthchecks.io /start URL on every run, unconditionally — this is
  the liveness signal ("the script still ran"), same role as in the weather app.
- If NO CFB markets are listed yet (the expected case until ~late Aug): ping
  the plain success URL too, so healthchecks.io stays quiet.
- The moment any market IS found: skip the success ping on purpose. There is
  no explicit /fail call — the check simply goes quiet, and healthchecks.io's
  own missed-ping alert (after the configured grace period) is what notifies
  the user. This deliberately reuses the alert path already trusted for the
  weather app rather than inventing a second one.
- Any exception (Kalshi API down, network error) also results in no success
  ping, which is indistinguishable from "markets appeared" until the user
  checks stdout/the log — this only matters as a will-fire-eventually signal,
  not as a diagnosis, so no attempt is made to disambiguate here.

Configure via environment variables (set in the scheduler, not committed):
    HEALTHCHECKS_URL   e.g. https://hc-ping.com/<uuid>   (required)

Series checked: KXNCAAFGAME (moneyline). This is the one referenced by
HANDOFF.md's Next Steps item 1a (verify away/home ticker order) — that's what
this check exists to unblock, so checking the other two captured series
(spread/total) isn't necessary for that purpose.
"""
from __future__ import annotations

import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from screen.snapshot import SERIES_MONEYLINE, fetch_series  # noqa: E402

PING_TIMEOUT_S = 10


def ping(url: str) -> None:
    try:
        requests.get(url, timeout=PING_TIMEOUT_S)
    except requests.RequestException as exc:
        # A dead healthchecks.io ping shouldn't crash the check itself —
        # print so it's visible in the scheduler's log, then keep going.
        print(f"WARNING: ping to {url} failed: {exc}", file=sys.stderr)


def main() -> int:
    base_url = os.environ.get("HEALTHCHECKS_URL")
    if not base_url:
        print("HEALTHCHECKS_URL is not set — refusing to run silently unmonitored.",
              file=sys.stderr)
        return 2

    ping(f"{base_url}/start")

    markets = fetch_series(SERIES_MONEYLINE)

    if not markets:
        print(f"{SERIES_MONEYLINE}: no markets listed yet.")
        ping(base_url)
        return 0

    print(f"{SERIES_MONEYLINE}: {len(markets)} market(s) found — CFB markets are live.")
    for m in markets[:10]:
        print(f"  {m.get('ticker')}  status={m.get('status')}")
    print("Success ping intentionally skipped — healthchecks.io will alert "
          "once the missed-ping grace period elapses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
