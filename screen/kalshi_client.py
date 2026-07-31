"""Thin READ-ONLY Kalshi market-data client.

Per CLAUDE.md: public endpoints at `https://external-api.kalshi.com/trade-api/v2`
require no auth, and this repo has **no Kalshi trading key and never will**.

This module is read-only *by construction*, not by discipline:

* It exposes exactly one HTTP verb — `get()`. There is no post/put/delete
  helper, and no `requests.Session` is handed out, so a caller cannot reach a
  mutating verb through this module without importing `requests` directly and
  obviously violating the project's hardest constraint.
* No auth is read, stored, or sent. Kalshi's order endpoints all require a
  signed key; with no key path in this module, order placement is not merely
  unimplemented but unreachable.

**Price fields:** CLAUDE.md requires the `*_dollars` fields (e.g.
`yes_bid_dollars`), NOT the deprecated integer-cent fields (`yes_bid`). The
cent fields are still present in responses, which is exactly why this is easy
to get wrong: reading `yes_bid` returns a plausible-looking number that is
100x off and will silently corrupt every edge computation downstream.
`prefer_dollars()` below centralizes that choice so it is made once.

Pagination is cursor-based: responses carry a `cursor` string, and an empty or
absent cursor means the last page. `get_paginated()` follows it, with a hard
page cap so a malformed cursor loop cannot spin forever.
"""
from __future__ import annotations

import time
from typing import Any, Iterator, Optional

import requests

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"

# Kalshi's published read limits are well above this; the delay exists to be a
# good citizen on a multi-page slate pull, not because a limit was observed.
REQUEST_DELAY_S = 0.15
MAX_PAGES = 200  # guard against a cursor that never terminates

# Deprecated integer-cent fields, kept here ONLY so callers can detect and
# reject them. Never read these for pricing.
DEPRECATED_CENT_FIELDS = frozenset(
    {"yes_bid", "yes_ask", "no_bid", "no_ask", "last_price",
     "previous_yes_bid", "previous_yes_ask", "previous_price"}
)


class KalshiError(RuntimeError):
    """Non-2xx from Kalshi, with the response body attached.

    Mirrors OddsPapiError's contract: fail loud with the body, so a wrong param
    name surfaces as a readable message instead of an opaque empty result.
    """

    def __init__(self, status: int, url: str, body: str):
        super().__init__(f"Kalshi {status} on {url}\n{body[:2000]}")
        self.status = status
        self.body = body


def get(path: str, **params: Any) -> dict:
    """GET {BASE_URL}{path} with `params`. Raises KalshiError on non-2xx.

    `None`-valued params are dropped rather than sent as the literal string
    "None" (the failure mode that makes a filter silently match nothing).
    """
    url = f"{BASE_URL}{path}"
    q = {k: v for k, v in params.items() if v is not None}
    resp = requests.get(url, params=q, timeout=30)
    if not resp.ok:
        raise KalshiError(resp.status_code, resp.url, resp.text)
    return resp.json()


def get_paginated(path: str, key: str, *, limit: int = 200,
                  max_pages: int = MAX_PAGES, **params: Any) -> Iterator[dict]:
    """Yield every item under `key` across all pages of a cursor endpoint.

    Stops on an empty/absent cursor, an empty page, or `max_pages`. A repeated
    cursor also stops the loop — Kalshi returning the same cursor twice would
    otherwise be an infinite pull of duplicate rows.
    """
    cursor: Optional[str] = None
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        payload = get(path, limit=limit, cursor=cursor, **params)
        batch = payload.get(key) or []
        if not batch:
            return
        yield from batch
        cursor = payload.get("cursor") or None
        if cursor is None or cursor in seen_cursors:
            return
        seen_cursors.add(cursor)
        time.sleep(REQUEST_DELAY_S)


def prefer_dollars(market: dict, field: str) -> Optional[float]:
    """Read `<field>_dollars` from a market dict, per CLAUDE.md.

    Returns None if the dollars field is absent rather than silently falling
    back to the deprecated cent field — a missing price must be visibly missing
    in the snapshot, not quietly replaced by a number 100x too large. Kalshi
    returns these as strings in some responses, so the cast is explicit.
    """
    raw = market.get(f"{field}_dollars")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
