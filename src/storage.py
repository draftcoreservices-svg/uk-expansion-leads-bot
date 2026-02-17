import hashlib
from sqlite_utils import Database


class Storage:
    def __init__(self, path: str = "cache.sqlite"):
        self.db = Database(path)
        self._init()

    def _init(self):
        self.db["seen"].create({
            "id": str,
            "lead_type": str,
            "title": str,
            "url": str,
            "first_seen": str,
        }, pk="id", if_not_exists=True)

        self.db["sponsors"].create({
            "name_norm": str,
        }, pk="name_norm", if_not_exists=True)

    @staticmethod
    def _hash(s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]

    def lead_id(self, lead_type: str, url: str, title: str) -> str:
        return self._hash(f"{lead_type}|{url}|{title}")

    def seen_before(self, lead_id: str) -> bool:
        return self.db["seen"].exists(lead_id)

    def mark_seen(self, lead_id: str, lead_type: str, title: str, url: str, first_seen: str) -> None:
        self.db["seen"].insert({
            "id": lead_id,
            "lead_type": lead_type,
            "title": title,
            "url": url,
            "first_seen": first_seen,
        }, pk="id", replace=False, ignore=True)

    @staticmethod
    def normalize_name(name: str) -> str:
        return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in name).split())

    def lookup_sponsor(self, name_norm: str):
        return self.db["sponsors"].get(name_norm) if self.db["sponsors"].exists(name_norm) else None
