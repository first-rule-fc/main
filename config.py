"""Project-wide configuration: paths and secrets.

Single source of truth for where data lives and how we read the CFBD key.
Everything downstream (ingestion, modeling, backtest) imports from here so
there are no scattered hard-coded paths.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Repo root = directory containing this file.
ROOT = Path(__file__).resolve().parent

# Load .env from repo root (no-op if absent; real env vars still win).
load_dotenv(ROOT / ".env")

# --- Data layout -----------------------------------------------------------
DATA = ROOT / "data"
DATA_RAW = DATA / "raw"            # optional cached raw pulls (gitignored)
DATA_PROCESSED = DATA / "processed"  # normalized parquet tables (gitignored)
RATINGS_DIR = DATA_PROCESSED / "ratings"  # per-source season rating tables

# Shadow-mode Screen/Scan output. APPEND-ONLY by convention: Kalshi prices
# cannot be reconstructed after the fact, so nothing in here is ever rewritten
# or regenerated — a lost snapshot is permanently lost gate data.
SNAPSHOTS = DATA / "snapshots"

for _p in (DATA_RAW, DATA_PROCESSED, RATINGS_DIR, SNAPSHOTS):
    _p.mkdir(parents=True, exist_ok=True)

# The human-facing ledger stays where it is; never migrated into a DB.
LEDGER_XLSX = ROOT / "docs" / "Kalshi_Ledger.xlsx"


def cfbd_api_key() -> str:
    """Return the CFBD API key or raise a clear, actionable error."""
    key = os.environ.get("CFBD_API_KEY")
    if not key:
        raise RuntimeError(
            "CFBD_API_KEY is not set. Add it to the .env file in the repo root "
            "(CFBD_API_KEY=your_key) or export it in your shell. "
            "Get a free key at https://collegefootballdata.com/key"
        )
    return key


def oddspapi_api_key() -> str:
    """Return the OddsPapi key or raise a clear, actionable error.

    Per CLAUDE.md: pass as the `apiKey` query param, NEVER a header.
    """
    key = os.environ.get("ODDSPAPI_API_KEY")
    if not key:
        raise RuntimeError(
            "ODDSPAPI_API_KEY is not set. Add it to the .env file in the repo "
            "root (ODDSPAPI_API_KEY=your_key) or export it in your shell. "
            "Get a key at https://oddspapi.io"
        )
    return key
