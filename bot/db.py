import os
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

CACHE_DIR = os.getenv('CACHE_DIR', '.cache')
DB_PATH = os.path.join(CACHE_DIR, 'state.db')


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect() -> sqlite3.Connection:
    os.makedirs(CACHE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    # Legacy tables
    conn.execute('CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, first_seen_utc TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)')

    # New tables
    conn.execute('''
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_utc TEXT,
            finished_utc TEXT,
            params_json TEXT,
            sponsor_new INTEGER,
            ch_candidates INTEGER,
            serp_calls INTEGER,
            verified_sites INTEGER
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            company_number TEXT PRIMARY KEY,
            name TEXT,
            incorporated TEXT,
            status TEXT,
            sic_codes TEXT,
            reg_address TEXT,
            reg_postcode TEXT,
            reg_town TEXT,
            reg_country TEXT,
            website TEXT,
            website_level TEXT,
            website_score INTEGER,
            emails TEXT,
            phones TEXT,
            last_enriched_utc TEXT,
            last_refreshed_utc TEXT
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS sponsor_rows (
            row_key TEXT PRIMARY KEY,
            org_name TEXT,
            town TEXT,
            county TEXT,
            route TEXT,
            subroute TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS sponsor_company_map (
            row_key TEXT PRIMARY KEY,
            company_number TEXT,
            match_score INTEGER,
            matched_utc TEXT
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            lead_id TEXT PRIMARY KEY,
            run_id TEXT,
            company_number TEXT,
            company_name TEXT,
            source TEXT,
            sponsor_route TEXT,
            score INTEGER,
            bucket TEXT,
            why TEXT,
            visa_hint TEXT,
            website TEXT,
            website_level TEXT,
            website_score INTEGER,
            emails_found TEXT,
            phones_found TEXT,
            enrich_status TEXT,
            created_utc TEXT
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            run_id TEXT,
            company_number TEXT,
            signal_type TEXT,
            weight INTEGER,
            evidence_json TEXT
        )
    ''')

    conn.commit()


# Legacy helpers (so you keep baseline behaviour)

def is_seen(conn: sqlite3.Connection, key: str) -> bool:
    cur = conn.execute('SELECT 1 FROM seen WHERE key=?', (key,))
    return cur.fetchone() is not None


def mark_seen(conn: sqlite3.Connection, key: str, ts_iso: str) -> None:
    conn.execute('INSERT OR IGNORE INTO seen(key, first_seen_utc) VALUES(?,?)', (key, ts_iso))


def meta_get(conn: sqlite3.Connection, k: str) -> str | None:
    cur = conn.execute('SELECT v FROM meta WHERE k=?', (k,))
    row = cur.fetchone()
    return row[0] if row else None


def meta_set(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute(
        'INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',
        (k, v)
    )


# New helpers

def run_start(conn: sqlite3.Connection, run_id: str, params: dict) -> None:
    conn.execute(
        'INSERT OR REPLACE INTO runs(run_id, started_utc, params_json, sponsor_new, ch_candidates, serp_calls, verified_sites) '
        'VALUES(?,?,?,?,?,?,?)',
        (run_id, utc_now_iso(), json.dumps(params, sort_keys=True), 0, 0, 0, 0)
    )


def run_finish(conn: sqlite3.Connection, run_id: str, sponsor_new: int, ch_candidates: int, serp_calls: int, verified_sites: int) -> None:
    conn.execute(
        'UPDATE runs SET finished_utc=?, sponsor_new=?, ch_candidates=?, serp_calls=?, verified_sites=? WHERE run_id=?',
        (utc_now_iso(), sponsor_new, ch_candidates, serp_calls, verified_sites, run_id)
    )


def upsert_company(conn: sqlite3.Connection, **fields) -> None:
    # minimal upsert via INSERT OR REPLACE preserving existing enrichment where not provided
    cn = fields.get('company_number')
    if not cn:
        return

    existing = conn.execute('SELECT website, website_level, website_score, emails, phones, last_enriched_utc FROM companies WHERE company_number=?', (cn,)).fetchone()
    if existing:
        for k, v in [('website','website'), ('website_level','website_level'), ('website_score','website_score'), ('emails','emails'), ('phones','phones'), ('last_enriched_utc','last_enriched_utc')]:
            if fields.get(k) in (None, ''):
                idx = {'website':0,'website_level':1,'website_score':2,'emails':3,'phones':4,'last_enriched_utc':5}[k]
                fields[k] = existing[idx]

    cols = [
        'company_number','name','incorporated','status','sic_codes','reg_address','reg_postcode','reg_town','reg_country',
        'website','website_level','website_score','emails','phones','last_enriched_utc','last_refreshed_utc'
    ]
    vals = [fields.get(c, '') for c in cols]
    placeholders = ','.join(['?']*len(cols))
    conn.execute(
        f'INSERT OR REPLACE INTO companies({",".join(cols)}) VALUES({placeholders})',
        vals
    )


def get_company(conn: sqlite3.Connection, company_number: str) -> dict | None:
    row = conn.execute('SELECT * FROM companies WHERE company_number=?', (company_number,)).fetchone()
    if not row:
        return None
    cols = [d[0] for d in conn.execute('PRAGMA table_info(companies)').fetchall()]
    # PRAGMA returns (cid,name,type,notnull,dflt,pk)
    cols = [c[1] for c in conn.execute('PRAGMA table_info(companies)').fetchall()]
    return dict(zip(cols, row))


def upsert_sponsor_row(conn: sqlite3.Connection, row_key: str, org_name: str, town: str, county: str, route: str, subroute: str, ts_iso: str) -> None:
    conn.execute(
        'INSERT INTO sponsor_rows(row_key, org_name, town, county, route, subroute, first_seen_utc, last_seen_utc) '
        'VALUES(?,?,?,?,?,?,?,?) '
        'ON CONFLICT(row_key) DO UPDATE SET last_seen_utc=excluded.last_seen_utc',
        (row_key, org_name, town, county, route, subroute, ts_iso, ts_iso)
    )


def get_sponsor_mapping(conn: sqlite3.Connection, row_key: str) -> tuple[str, int] | None:
    row = conn.execute('SELECT company_number, match_score FROM sponsor_company_map WHERE row_key=?', (row_key,)).fetchone()
    return (row[0], int(row[1])) if row else None


def set_sponsor_mapping(conn: sqlite3.Connection, row_key: str, company_number: str, match_score: int, ts_iso: str) -> None:
    conn.execute(
        'INSERT OR REPLACE INTO sponsor_company_map(row_key, company_number, match_score, matched_utc) VALUES(?,?,?,?)',
        (row_key, company_number, int(match_score), ts_iso)
    )


def add_signal(conn: sqlite3.Connection, run_id: str, company_number: str, signal_type: str, weight: int, evidence: dict) -> None:
    conn.execute(
        'INSERT INTO signals(run_id, company_number, signal_type, weight, evidence_json) VALUES(?,?,?,?,?)',
        (run_id, company_number, signal_type, int(weight), json.dumps(evidence, sort_keys=True)[:8000])
    )


def insert_lead(conn: sqlite3.Connection, lead: dict) -> None:
    cols = [
        'lead_id','run_id','company_number','company_name','source','sponsor_route','score','bucket','why','visa_hint',
        'website','website_level','website_score','emails_found','phones_found','enrich_status','created_utc'
    ]
    vals = [lead.get(c, '') for c in cols]
    placeholders = ','.join(['?']*len(cols))
    conn.execute(f'INSERT OR REPLACE INTO leads({",".join(cols)}) VALUES({placeholders})', vals)
