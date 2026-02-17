import hashlib
from sqlite_utils import Database


class Storage:
    def __init__(self, path: str = "cache.sqlite"):
        self.db = Database(path)
        self._init()

    def _init(self) -> None:
        # dedupe table
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

        # metadata key/value store
        self.db["meta"].create(
            {
                "k": str,
                "v": str,
            },
            pk="k",
            if_not_exists=True,
        )

        # sponsor register table (loaded from GOV.UK CSV)
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

    def seen_before(self, lead_id: str) -> bool:
        return self.db["seen"].exists(lead_id)

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

    # -------- meta helpers --------
    def get_meta(self, key: str):
        tbl = self.db["meta"]
        if tbl.exists(key):
            return tbl.get(key)["v"]
        return None

    def upsert_meta(self, key: str, value: str) -> None:
        self.db["meta"].insert({"k": key, "v": value}, pk="k", replace=True)

    # -------- sponsor register lookup --------
    @staticmethod
    def normalize_name(name: str) -> str:
        # Lowercase, keep alnum, collapse whitespace
        cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in (name or ""))
        return " ".join(cleaned.split())

    def sponsor_lookup(self, org_name: str):
        name_norm = self.normalize_name(org_name)
        tbl = self.db["sponsor_register"]
        if tbl.exists(name_norm):
            return tbl.get(name_norm)
        return None
