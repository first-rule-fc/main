"""Phase 4 — shadow-mode Kalshi price snapshot logger.

Captures current Kalshi CFB market prices to disk on a schedule. **This is the
deadline-critical component of the whole project**: Kalshi entry prices cannot
be reconstructed after the fact, so any game week that runs without this logger
is permanently lost data for Gate B. Model P(win), by contrast, can be refit
from CFBD data at any time — which is why this ships before the edge layer.

SHADOW MODE — this module deliberately imports nothing from `ratings/` and
computes no edge. Per CLAUDE.md, Screen/Scan output must feed nothing (not
Analyze, not Seeker, not the Ledger) until Gate A passes. That constraint is
enforced structurally here: there is no model import to accidentally wire up.

READ-ONLY: goes through `screen.kalshi_client`, which exposes no mutating verb
and no auth path. No order can be placed from this module.

Design decisions (documented because a wrong default here loses data
irrecoverably rather than failing loudly):

* **Raw JSON is written verbatim alongside the normalized parquet.** The
  normalizer below encodes assumptions about Kalshi's schema that could be
  wrong. If they are, raw responses can be re-normalized later; a price never
  captured is gone forever. Storage is trivial next to that risk. The raw file
  is the actual deliverable — the parquet is a convenience view of it.

* **Series are selected by explicit ticker, never keyword match.** The
  discovery run matched 18 "CFB" markets that were all MLB/MLS parlays: Kalshi
  embeds random hex in multivariate-event tickers, and `...S202620ACFB794A2`
  contains the substring "cfb". Keyword filtering is unsafe here.

* **Moneyline, spread AND total are all captured.** The model natively produces
  a margin, so the spread series is arguably a closer fit to it than the
  moneyline; totals are not modeled at all yet but cost one extra call to
  capture. Since none of these prices can be reconstructed later, breadth now
  is cheap insurance against a modeling decision made mid-season.

* **`*_dollars` fields only**, read through `prefer_dollars()`, which returns
  None rather than falling back to a cent field. Kalshi returns these as
  STRINGS (`'0.6200'`), confirmed in discovery — hence the explicit cast.

* **Volume/open-interest carry an `_fp` suffix** (`volume_fp`,
  `open_interest_fp`). Plain `volume`/`open_interest` do not exist on this API
  version; reading them would have silently logged nulls forever.

* **Append-only, one file per run.** Nothing is ever overwritten or
  regenerated. A re-run writes a new timestamped pair rather than replacing an
  existing one, so a buggy run cannot destroy a good earlier capture.

* **Status is recorded, not filtered on.** Discovery showed the `status=open`
  QUERY value returns markets whose `status` FIELD reads `"active"` — the two
  vocabularies differ. Filtering on a guessed status value would silently drop
  the whole slate, so this records whatever comes back and filters nothing.

Usage (run from your own terminal — the sandboxed shell cannot reach HTTPS):
    .venv\\Scripts\\python.exe -m screen.snapshot                  # weekly batch
    .venv\\Scripts\\python.exe -m screen.snapshot --window-hours 6 # near-kickoff
    .venv\\Scripts\\python.exe -m screen.snapshot --dry-run        # no files written
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd

from config import SNAPSHOTS
from screen.kalshi_client import KalshiError, get_paginated, prefer_dollars

try:  # stdlib on 3.9+; the fallback keeps a missing tzdata from killing a run
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo("America/Chicago")
except Exception:  # pragma: no cover - only on a broken tzdata install
    CENTRAL = timezone(timedelta(hours=-5))

# Confirmed in the 2026-07-24 discovery run against /series.
SERIES_MONEYLINE = "KXNCAAFGAME"    # maps directly onto model P(win)
SERIES_SPREAD = "KXNCAAFSPREAD"     # closest fit to the model's native output
SERIES_TOTAL = "KXNCAAFTOTAL"       # not modeled yet; captured for optionality

# All three are captured because the model natively produces MARGIN, so the
# spread series is arguably a better fit for it than the moneyline, and none of
# these prices can be reconstructed later. The marginal cost is one paginated
# call each; the cost of not having them is a whole season of missing data.
DEFAULT_SERIES = (SERIES_MONEYLINE, SERIES_SPREAD, SERIES_TOTAL)

# Deliberately NOT captured: KXNCAAF1HWINNER (first-half markets are a
# different modeling problem entirely) and KXNCAAFCSGAME (FCS — the model rates
# FBS teams only and floors FBS-FCS games rather than modeling them).
MARKET_KIND = {SERIES_MONEYLINE: "moneyline", SERIES_SPREAD: "spread",
               SERIES_TOTAL: "total"}

# Series where the away-then-home team order has been directly checked against
# CFBD's home_team/away_team fields (see parse_ticker's docstring). Spread and
# total share the same team-blob parsing but were empty when this was checked
# 2026-07-31, so they're deliberately left off this list rather than assumed.
CONFIRMED_AWAY_HOME_SERIES = {SERIES_MONEYLINE}

# Price/size fields read via prefer_dollars() (all *_dollars, all strings).
DOLLAR_FIELDS = ("yes_bid", "yes_ask", "no_bid", "no_ask", "last_price",
                 "previous_price", "previous_yes_bid", "previous_yes_ask",
                 "liquidity", "notional_value")
# Floating-point-suffixed fields, confirmed present in discovery.
FP_FIELDS = ("volume_fp", "volume_24h_fp", "open_interest_fp",
             "yes_bid_size_fp", "yes_ask_size_fp")
PASSTHROUGH = ("ticker", "event_ticker", "market_type", "status", "title",
               "yes_sub_title", "no_sub_title", "open_time", "close_time",
               "expected_expiration_time", "expiration_time", "result",
               "can_close_early", "strike_type", "price_level_structure")

# Real Kalshi CFB ticker: KXNCAAFGAME-26SEP19MSUND-ND
#                                      ^YY^MON^DD  ^both teams  ^YES side
# NO hhmm segment — confirmed 2026-07-31 against all 32 live KXNCAAFGAME
# markets (0/32 matched the original regex, which required a 4-digit hhmm
# block modeled on an assumed-but-never-observed example). `hhmm` is now
# OPTIONAL: kept for whatever series does encode it (unconfirmed pre-2026-07-31
# assumption, not re-checked here) without breaking the CFB shape that doesn't.
# The side class MUST allow "." — half-point strikes (a 52.5 total, a -7.5
# spread) are the norm in football, and an [A-Z0-9]-only class silently failed
# the whole match on them, losing the date and teams too, not just the strike.
TICKER_RE = re.compile(
    r"^(?P<series>[A-Z0-9]+)-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<dd>\d{2})"
    r"(?:(?P<hhmm>\d{4}))?(?P<teams>[A-Z0-9]+)-(?P<side>[A-Z0-9.]+)$"
)
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
     "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1)}


def _to_float(raw: Any) -> Optional[float]:
    """Cast an `_fp` field, which Kalshi returns as a string like '0.00'."""
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def parse_ticker(ticker: str) -> dict:
    """Best-effort decode of a ticker into date / teams / YES side / strike.

    Parsing is deliberately split into two independent stages, because the
    three captured series encode their side suffix differently and a single
    all-or-nothing parse would throw away the half that did work:

      moneyline  KXNCAAFGAME-26AUG291900ALAWIS-ALA    side = a team code
      spread     KXMLBSPREAD-26JUL241610COLMIL-MIL2   side = team + strike
      total      KXMLBTOTAL-26JUL241845NYYPHI-8       side = strike only

    Stage 1 (`ticker_parse_ok`) decodes the date/time/teams blob, which is
    common to all three. Stage 2 (`teams_split_ok`) splits that blob into two
    team codes, which is only possible when the side suffix names a team — so
    it is expected to be False on every totals market, and that is not an
    error. An earlier single-stage version returned nothing at all for spreads
    and totals.

    Every derived field is None on failure and the raw ticker is still
    recorded: an unparsed ticker must never drop a price row, since the price
    is the irreplaceable part and the parse can be redone from the raw file.

    **One assumption confirmed, one still open:**
    * The two-team blob is read as AWAY-then-HOME. Originally inferred from a
      single MLB example (`...CHCPIT-CHC`) with no CFB market to check it
      against. **Confirmed 2026-07-31 against CFBD's own home/away fields on
      3/3 real KXNCAAFGAME fixtures** (MSU@ND, LSU@Ole Miss, Houston@Texas
      Tech — `team_first` was CFBD's `away_team` and `team_second` was
      `home_team` in all three). `home_away_assumed` is now False for
      KXNCAAFGAME rows specifically (see `CONFIRMED_AWAY_HOME_SERIES` below);
      it stays True for every other series (MLB, and CFB spread/total, whose
      series were empty at verification time and haven't been directly
      checked) since the ordering could in principle differ per series even
      though the parsing code is shared.
    * The strike's sign/direction convention on spreads is UNKNOWN — `MIL2`
      could mean Milwaukee +2 or -2. `strike_raw` keeps the original string so
      this can be settled from a real market rather than guessed now.
    """
    out: dict[str, Any] = {
        "ticker_parse_ok": False, "teams_split_ok": False, "series": None,
        "market_kind": None, "game_date": None, "kickoff_hhmm_utc": None,
        "teams_raw": None, "team_first": None, "team_second": None,
        "side_raw": None, "yes_team": None, "yes_is_first": None,
        "strike_raw": None, "strike": None, "home_away_assumed": True,
    }
    m = TICKER_RE.match(ticker or "")
    if not m:
        return out
    mon = MONTHS.get(m.group("mon"))
    if mon is None:
        return out

    teams, side = m.group("teams"), m.group("side")
    series = m.group("series")
    out.update({
        "ticker_parse_ok": True,
        "series": series,
        "market_kind": MARKET_KIND.get(series, "other"),
        "game_date": f"20{m.group('yy')}-{mon:02d}-{m.group('dd')}",
        "kickoff_hhmm_utc": m.group("hhmm"),
        "teams_raw": teams,
        "side_raw": side,
    })

    # Split the side suffix into an optional team code and an optional strike.
    sm = re.match(r"^(?P<team>[A-Z]*)(?P<strike>\d*\.?\d*)$", side)
    if not sm:
        return out
    team_part, strike_part = sm.group("team"), sm.group("strike")
    if strike_part:
        out["strike_raw"] = strike_part
        out["strike"] = _to_float(strike_part)
    if not team_part:
        return out  # totals: no team in the suffix, so no split is possible

    # Team codes are variable-length and concatenated, so the side's team code
    # is the only key available to split them. A suffix matching BOTH ends is
    # genuinely ambiguous and is reported unsplit rather than guessed.
    starts, ends = teams.startswith(team_part), teams.endswith(team_part)
    if starts and not ends:
        first, second, yes_is_first = team_part, teams[len(team_part):], True
    elif ends and not starts:
        first, second, yes_is_first = teams[:-len(team_part)], team_part, False
    else:
        return out
    if not first or not second:
        return out

    out.update({"teams_split_ok": True, "team_first": first,
                "team_second": second, "yes_team": team_part,
                "yes_is_first": yes_is_first,
                "home_away_assumed": series not in CONFIRMED_AWAY_HOME_SERIES})
    return out


def normalize(market: dict, captured_utc: str, captured_ct: str) -> dict:
    """Flatten one raw market into a snapshot row.

    Deliberately lossy ONLY in the parquet view — the raw response for this
    market is written verbatim to the companion .json file, so anything this
    function drops or mis-reads stays recoverable.
    """
    row: dict[str, Any] = {
        "captured_utc": captured_utc,
        "captured_CT": captured_ct,
    }
    for f in PASSTHROUGH:
        row[f] = market.get(f)
    for f in DOLLAR_FIELDS:
        row[f"{f}_dollars"] = prefer_dollars(market, f)
    for f in FP_FIELDS:
        row[f] = _to_float(market.get(f))
    row.update(parse_ticker(market.get("ticker", "")))

    # Mid price is a convenience only, and is None unless BOTH sides exist —
    # a one-sided book must not be silently reported as a tradeable midpoint.
    bid, ask = row.get("yes_bid_dollars"), row.get("yes_ask_dollars")
    row["yes_mid_dollars"] = (
        round((bid + ask) / 2, 4) if bid is not None and ask is not None else None
    )
    row["gate_status"] = "PRE_GATE_A"  # shadow mode; see CLAUDE.md ledger schema
    return row


def fetch_series(series_ticker: str) -> list[dict]:
    """All markets for one series, raw and unfiltered.

    No status filter is applied: discovery showed the query vocabulary
    (`status=open`) and the response vocabulary (`status: "active"`) differ, and
    a wrong guess here silently returns an empty slate.
    """
    return list(get_paginated("/markets", "markets", series_ticker=series_ticker))


def within_window(market: dict, hours: Optional[float], now: datetime) -> bool:
    """True if the market closes within `hours` — used for near-kickoff runs.

    A market with an unparseable/absent close time is KEPT, not dropped. Losing
    a price because a timestamp failed to parse is the exact irreversible
    failure this module exists to prevent.
    """
    if hours is None:
        return True
    raw = market.get("close_time") or market.get("expected_expiration_time")
    if not raw:
        return True
    try:
        close = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return True
    return close <= now + timedelta(hours=hours)


def run(series: tuple[str, ...] = DEFAULT_SERIES, *,
        window_hours: Optional[float] = None, dry_run: bool = False) -> pd.DataFrame:
    now_utc = datetime.now(timezone.utc)
    captured_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    captured_ct = now_utc.astimezone(CENTRAL).strftime("%Y-%m-%d %H:%M:%S")
    stamp = now_utc.astimezone(CENTRAL).strftime("%Y%m%d_%H%M%S")

    raw_by_series: dict[str, list[dict]] = {}
    rows: list[dict] = []
    for s in series:
        try:
            markets = fetch_series(s)
        except KalshiError as e:
            # One dead series must not abort the run — the other series' prices
            # are still unreconstructible and worth saving.
            print(f"  {s}: FAILED — {e}")
            raw_by_series[s] = []
            continue
        kept = [m for m in markets if within_window(m, window_hours, now_utc)]
        raw_by_series[s] = kept
        rows.extend(normalize(m, captured_utc, captured_ct) for m in kept)
        print(f"  {s}: {len(markets)} markets, {len(kept)} in window")

    df = pd.DataFrame(rows)

    if dry_run:
        print("\n[dry-run] nothing written.")
        return df
    if not rows:
        # Still record the empty pull: "we looked and there was nothing" is a
        # real observation, and distinguishing it from "the job never ran" is
        # what makes a coverage gap diagnosable later.
        print("\nNo markets returned — writing raw file anyway to record the "
              "attempt (an empty pull and a missed run must stay distinguishable).")

    raw_path = SNAPSHOTS / f"kalshi_cfb_{stamp}_CT.raw.json"
    raw_path.write_text(json.dumps({
        "captured_utc": captured_utc, "captured_CT": captured_ct,
        "series": list(series), "window_hours": window_hours,
        "markets": raw_by_series,
    }, indent=2), encoding="utf-8")
    print(f"\nraw  -> {raw_path}")

    if rows:
        pq_path = SNAPSHOTS / f"kalshi_cfb_{stamp}_CT.parquet"
        df.to_parquet(pq_path, index=False)
        print(f"rows -> {pq_path}  ({len(df)} rows)")

    return df


def _report(df: pd.DataFrame) -> None:
    if df.empty:
        return
    print("-" * 68)
    ok = int(df["ticker_parse_ok"].sum())
    print(f"ticker parse: {ok}/{len(df)} decoded"
          + ("" if ok == len(df) else "  <- inspect the raw file for the rest"))
    split = int(df["teams_split_ok"].sum())
    n_total = int((df["market_kind"] == "total").sum())
    print(f"team split:   {split}/{len(df)} "
          f"(expected to fail on all {n_total} totals — their suffix is a "
          f"strike, not a team)")
    priced = int(df["yes_mid_dollars"].notna().sum())
    print(f"two-sided books: {priced}/{len(df)}")
    print("by market kind: "
          + ", ".join(f"{k}={v}" for k, v in df["market_kind"]
                      .value_counts(dropna=False).items()))
    if split:
        print("\nsample decoded tickers "
              "(VERIFY away/home order against a real game before trusting sides):")
        cols = ["ticker", "market_kind", "game_date", "team_first",
                "team_second", "yes_team", "strike_raw",
                "yes_bid_dollars", "yes_ask_dollars"]
        print(df[df["teams_split_ok"]][cols].head(8).to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Shadow-mode Kalshi CFB price snapshot (read-only, no auth).")
    ap.add_argument("--series", nargs="+", default=list(DEFAULT_SERIES),
                    help=f"series tickers (default: {' '.join(DEFAULT_SERIES)})")
    ap.add_argument("--window-hours", type=float, default=None,
                    help="only markets closing within N hours (near-kickoff run)")
    ap.add_argument("--dry-run", action="store_true", help="fetch but write nothing")
    args = ap.parse_args()

    print("=" * 68)
    print("KALSHI CFB SNAPSHOT — SHADOW MODE (feeds nothing downstream)")
    print("=" * 68)
    df = run(tuple(args.series), window_hours=args.window_hours,
             dry_run=args.dry_run)
    _report(df)


if __name__ == "__main__":
    main()
