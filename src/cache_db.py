from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS emailed_leads (
  company_number TEXT PRIMARY KEY,
  company_name TEXT,
  emailed_at TEXT
);
"""


class LeadCache:
    def __init__(self, path: str):
        self.path = path
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute(SCHEMA)
        self._conn.commit()

    def close(self):
        self._conn.close()

    def has(self, company_number: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM emailed_leads WHERE company_number = ? LIMIT 1",
            (company_number,),
        )
        return cur.fetchone() is not None

    def add_many(self, items: Iterable[tuple[str, str]]):
        now = datetime.now(timezone.utc).isoformat()
        rows = [(cn, name, now) for cn, name in items]
        self._conn.executemany(
            "INSERT OR IGNORE INTO emailed_leads(company_number, company_name, emailed_at) VALUES (?,?,?)",
            rows,
        )
        self._conn.commit()
