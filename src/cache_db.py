from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS emailed_leads (
  company_number TEXT PRIMARY KEY,
  company_name   TEXT,
  emailed_at     TEXT
);

CREATE TABLE IF NOT EXISTS seen_companies (
  company_number TEXT PRIMARY KEY,
  seen_at        TEXT
);
"""

# A company we have already emailed stays out of the pool for 180 days.
# After that it re-enters — circumstances may have changed.
EMAILED_TTL_DAYS = 180

# A company we evaluated but rejected is skipped for the full 365-day window.
# Once it rolls off this cache it will be re-evaluated from scratch.
SEEN_TTL_DAYS = 365


class LeadCache:
    def __init__(self, path: str):
        self.path = path
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        for stmt in SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._conn.execute(stmt)
        self._conn.commit()
        self._prune()

    def _prune(self) -> None:
        """Remove stale entries from both tables on startup."""
        now = datetime.now(timezone.utc)

        emailed_cutoff = (now - timedelta(days=EMAILED_TTL_DAYS)).isoformat()
        self._conn.execute(
            "DELETE FROM emailed_leads WHERE emailed_at < ?", (emailed_cutoff,)
        )

        seen_cutoff = (now - timedelta(days=SEEN_TTL_DAYS)).isoformat()
        self._conn.execute(
            "DELETE FROM seen_companies WHERE seen_at < ?", (seen_cutoff,)
        )

        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ #
    # emailed_leads — companies we have already sent to Rushi             #
    # ------------------------------------------------------------------ #

    def was_emailed(self, company_number: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM emailed_leads WHERE company_number = ? LIMIT 1",
            (company_number,),
        )
        return cur.fetchone() is not None

    def add_emailed(self, items: Iterable[tuple[str, str]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        rows = [(cn, name, now) for cn, name in items]
        self._conn.executemany(
            "INSERT OR REPLACE INTO emailed_leads(company_number, company_name, emailed_at) VALUES (?,?,?)",
            rows,
        )
        self._conn.commit()

    # ------------------------------------------------------------------ #
    # seen_companies — every company number we have ever evaluated        #
    # ------------------------------------------------------------------ #

    def was_seen(self, company_number: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM seen_companies WHERE company_number = ? LIMIT 1",
            (company_number,),
        )
        return cur.fetchone() is not None

    def mark_seen(self, company_numbers: Iterable[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        rows = [(cn, now) for cn in company_numbers]
        self._conn.executemany(
            "INSERT OR IGNORE INTO seen_companies(company_number, seen_at) VALUES (?,?)",
            rows,
        )
        self._conn.commit()

    # ------------------------------------------------------------------ #
    # Legacy shim — keeps any code that still calls .has() working        #
    # ------------------------------------------------------------------ #

    def has(self, company_number: str) -> bool:
        return self.was_emailed(company_number) or self.was_seen(company_number)

    def add_many(self, items: Iterable[tuple[str, str]]) -> None:
        self.add_emailed(items)
