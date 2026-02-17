import os
import sqlite3
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta

CACHE_DIR = ".cache"
DB_PATH = os.path.join(CACHE_DIR, "state.db")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Store:
    def __init__(self):
        os.makedirs(CACHE_DIR, exist_ok=True)
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        c = self.conn
        c.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, first_seen_utc TEXT)")
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                lead_key TEXT PRIMARY KEY,
                company_number TEXT,
                company_name TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                source TEXT,
                score INTEGER,
                bucket TEXT,
                case_type TEXT,
                visa_hint TEXT,
                why TEXT,
                reg_address TEXT,
                reg_postcode TEXT,
                reg_town TEXT,
                incorporated TEXT,
                website TEXT,
                website_confidence INTEGER,
                emails TEXT,
                phones TEXT,
                contact_source_url TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS actions (
                lead_key TEXT PRIMARY KEY,
                status TEXT,
                note TEXT,
                updated_utc TEXT
            )
            """
        )
        c.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()

    def meta_get(self, k: str) -> Optional[str]:
        r = self.conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r["v"] if r else None

    def meta_set(self, k: str, v: str):
        self.conn.execute(
            "INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (k, v),
        )

    def is_seen(self, key: str) -> bool:
        r = self.conn.execute("SELECT 1 FROM seen WHERE key=?", (key,)).fetchone()
        return r is not None

    def mark_seen(self, key: str, ts_iso: str):
        self.conn.execute("INSERT OR IGNORE INTO seen(key, first_seen_utc) VALUES(?,?)", (key, ts_iso))

    def action_get(self, lead_key: str) -> Optional[Dict[str, Any]]:
        r = self.conn.execute("SELECT * FROM actions WHERE lead_key=?", (lead_key,)).fetchone()
        return dict(r) if r else None

    def action_set(self, lead_key: str, status: str, note: str = ""):
        self.conn.execute(
            "INSERT INTO actions(lead_key, status, note, updated_utc) VALUES(?,?,?,?) "
            "ON CONFLICT(lead_key) DO UPDATE SET status=excluded.status, note=excluded.note, updated_utc=excluded.updated_utc",
            (lead_key, status, note, utc_now_iso()),
        )

    def is_do_not_contact(self, lead_key: str) -> bool:
        a = self.action_get(lead_key)
        return bool(a and (a.get("status") or "").upper() == "DO_NOT_CONTACT")

    def upsert_lead(self, lead: Dict[str, Any]):
        now = utc_now_iso()
        lead_key = lead["lead_key"]
        existing = self.conn.execute("SELECT first_seen_utc FROM leads WHERE lead_key=?", (lead_key,)).fetchone()
        first_seen = existing["first_seen_utc"] if existing else now
        self.conn.execute(
            """
            INSERT INTO leads(
                lead_key, company_number, company_name, first_seen_utc, last_seen_utc, source,
                score, bucket, case_type, visa_hint, why,
                reg_address, reg_postcode, reg_town, incorporated,
                website, website_confidence, emails, phones, contact_source_url
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(lead_key) DO UPDATE SET
                company_number=excluded.company_number,
                company_name=excluded.company_name,
                last_seen_utc=excluded.last_seen_utc,
                source=excluded.source,
                score=excluded.score,
                bucket=excluded.bucket,
                case_type=excluded.case_type,
                visa_hint=excluded.visa_hint,
                why=excluded.why,
                reg_address=excluded.reg_address,
                reg_postcode=excluded.reg_postcode,
                reg_town=excluded.reg_town,
                incorporated=excluded.incorporated,
                website=excluded.website,
                website_confidence=excluded.website_confidence,
                emails=excluded.emails,
                phones=excluded.phones,
                contact_source_url=excluded.contact_source_url
            """,
            (
                lead_key,
                lead.get("company_number", ""),
                lead.get("company_name", ""),
                first_seen,
                now,
                lead.get("source", ""),
                int(lead.get("score", 0)),
                lead.get("bucket", ""),
                lead.get("case_type", ""),
                lead.get("visa_hint", ""),
                lead.get("why", ""),
                lead.get("reg_address", ""),
                lead.get("reg_postcode", ""),
                lead.get("reg_town", ""),
                lead.get("incorporated", ""),
                lead.get("website", ""),
                int(lead.get("website_confidence") or 0),
                lead.get("emails", ""),
                lead.get("phones", ""),
                lead.get("contact_source_url", ""),
            ),
        )

    def fetch_recent_leads(self, days: int = 30, limit: int = 400) -> List[Dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat()
        rows = self.conn.execute(
            "SELECT * FROM leads WHERE last_seen_utc >= ? ORDER BY score DESC, last_seen_utc DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]
