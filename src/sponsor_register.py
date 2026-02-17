from typing import Optional
from .storage import Storage


def is_on_sponsor_register(storage: Storage, company_name: str) -> Optional[bool]:
    """
    Returns:
      True  = known on register
      False = known not on register (we don't generally have this)
      None  = unknown (default)
    """
    normalized = storage.normalize_name(company_name)
    hit = storage.lookup_sponsor(normalized)
    if hit is None:
        return None
    return True
