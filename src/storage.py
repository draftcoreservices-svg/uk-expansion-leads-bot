import hashlib
from typing import Optional, Any, Dict
from sqlite_utils import Database


class Storage:
    """
    SQLite-backed storage for:
      - deduping seen leads
      - simple key/value metadata
      - sponsor register lookup (loaded from GOV.UK CSV)

    Notes on sqlite-utils:
      - Table.exists() checks whether the TABLE exists, not whether a ROW exists.
      - Use table.get(pk, default=...) or try/except around table.get(pk) for row existence.
    """

    def __init__(self, path: str = "cache.sqlite"):
        self.db = Database(path)
        self._init()

    def _init(self) -> None:
        # Dedupe table
        self.db["seen"].create(
            {
                "id": str,
                "lead_type": str,
                "title": str,
                "url": str,
                "first_seen": str,
            },
            pk="id",
            if_not_exists=True,
        )

        # Metadata key/value store
        self.db["meta"].create(
            {
                "k": str,
                "v": str,
            },
            pk="k",
            if_not_exists=True,
        )

        # Sponsor register table (loaded from GOV.UK CSV)
        self.db["sponsor_register"].create(
            {
                "name_norm": str,
                "org_name": str,
                "town": str,
                "county": str,
                "type_rating": str,
                "route": str,
                "source_date": str,
            },
            pk="name_norm",
            if_not_exists=True,
        )

    @staticmethod
    def _hash(s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]

    def lead_id(self, lead_type: str, url: str, title: str) -> str:
        return self._hash(f"{lead_type}|{url}|{title}")

    # -------------------------
    # Seen-lead dedupe helpers
    # -------------------------
    def seen_before(self, lead_id: str) -> bool:
        """
        Returns True if the lead_id row exists in the 'seen' table.
        Uses pk lookup (not Table.exists()).
        """
        tbl = self.db["seen"]
        try:
            # sqlite-utils supports default= in recent versions; keep robust fallback.
            row = tbl.get(lead_id, default=None)
            return row is not None
        except TypeError:
            # If default= isn't supported in some older build, fallback.
            try:
                tbl.get(lead_id)
                return True
            except Exception:
                return False

    def mark_seen(self, lead_id: str, lead_type: str, title: str, url: str, first_seen: str) -> None:
        self.db["seen"].insert(
            {
                "id": lead_id,
                "lead_type": lead_type,
                "title": title,
                "url": url,
                "first_seen": first_seen,
            },
            pk="id",
            replace=False,
            ignore=True,
        )

    # -------------------------
    # Meta helpers
    # -------------------------
    def get_meta(self, key: str) -> Optional[str]:
        tbl = self.db["meta"]
        try:
            row = tbl.get(key, default=None)
            if row is None:
                return None
            return row.get("v")
        except TypeError:
            try:
                return tbl.get(key).get("v")
            except Exception:
                return None

    def upsert_meta(self, key: str, value: str) -> None:
        self.db["meta"].insert({"k": key, "v": value}, pk="k", replace=True)

    # -------------------------
    # Sponsor register lookup
    # -------------------------
    @staticmethod
    def normalize_name(name: str) -> str:
        cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in (name or ""))
        return " ".join(cleaned.split())

    def sponsor_lookup(self, org_name: str) -> Optional[Dict[str, Any]]:
        """
        Returns the sponsor register row dict if present; otherwise None.
        Uses pk lookup (not Table.exists()).
        """
        name_norm = self.normalize_name(org_name)
        tbl = self.db["sponsor_register"]
        try:
            return tbl.get(name_norm, default=None)
        except TypeError:
            try:
                return tbl.get(name_norm)
            except Exception:
                return None
