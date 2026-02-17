"""CW UK Expansion Leads Bot (refactored)

Internal BD tooling only. Always verify primary sources before outreach.

Entry point: python -m bot.main
"""

from __future__ import annotations

import os
from datetime import timedelta

from .config import Config, require_env
from .http import make_session
from . import db

from .sources import sponsor_register as sponsor
from .sources import companies_house as ch
from .matching import best_ch_match_for_sponsor
from .utils import utc_now, clean_display_name, norm, norm_upper, is_uk_country, looks_like_subsidiary_name
from .scoring import compute_score, visa_hint
from .enrich import enrich
from .report import html_report, leads_to_csv_bytes
from .emailer import send_email


def _sponsor_filter(rows: list[dict], cfg: Config):
    filtered = []
    for row in rows:
        f = sponsor.row_fields(row)
        if f['route'] not in cfg.sponsor_route_allowlist:
            continue
        if sponsor.is_noise(f['name'], cfg.min_clean_name_len, cfg.max_non_alnum_ratio):
            continue
        filtered.append((row, f))
    return filtered


def _officer_signals(officers: list[dict], cfg: Config) -> dict:
    foreign_addr = 0
    foreign_res = 0
    foreign_nat = 0
    countries = set()
    res_countries = set()
    nats = set()

    for o in officers or []:
        addr = o.get('address') or {}
        c = norm(addr.get('country') or '')
        if c:
            countries.add(c.title())
        if c and not is_uk_country(c):
            foreign_addr += 1

        cor = norm(o.get('country_of_residence') or '')
        if cor:
            res_countries.add(cor.title())
        if cor and not is_uk_country(cor):
            foreign_res += 1

        nat = norm(o.get('nationality') or '')
        if nat:
            nats.add(nat.title())
        # nationality isn't always a country name; use a loose heuristic: if not UK/English/Scottish etc treat as non-UK
        nat_u = norm_upper(nat)
        if nat_u and nat_u not in {'BRITISH', 'ENGLISH', 'SCOTTISH', 'WELSH', 'NORTHERN IRISH', 'IRISH'}:
            foreign_nat += 1

    # small boost flag for priority countries
    priority = any(norm_upper(x) in cfg.priority_countries for x in countries | res_countries)

    return {
        'foreign_officer_address': foreign_addr,
        'foreign_officer_residence': foreign_res,
        'foreign_officer_nationality': foreign_nat,
        'address_countries': sorted(countries),
        'residence_countries': sorted(res_countries),
        'nationalities': sorted(nats),
        'priority_country_seen': priority,
    }


def _psc_signals(psc_items: list[dict], cfg: Config) -> dict:
    foreign_corporate = 0
    foreign_countries = set()
    corporate_names = []

    for p in psc_items or []:
        kind = (p.get('kind') or '').lower()
        name = clean_display_name(p.get('name') or '')
        addr = p.get('address') or {}
        country = norm(addr.get('country') or '')

        is_corp = any(k in kind for k in ['corporate-entity', 'legal-person', 'corporate', 'other-registrable-person'])
        if is_corp:
            if name:
                corporate_names.append(name)
            if country:
                foreign_countries.add(country.title())
            if country and not is_uk_country(country):
                foreign_corporate += 1

    return {
        'foreign_corporate_psc': foreign_corporate,
        'psc_countries': sorted(foreign_countries),
        'psc_corporates': corporate_names[:4],
        'has_foreign_psc': foreign_corporate >= 1,
    }


def main():
    cfg = Config()

    # Required secrets
    require_env('COMPANIES_HOUSE_API_KEY')
    serp_key = require_env('SERPAPI_API_KEY')
    require_env('SMTP_HOST'); require_env('SMTP_USER'); require_env('SMTP_PASS'); require_env('EMAIL_FROM'); require_env('EMAIL_TO')

    http = make_session(retry_total=cfg.ch_retry_count)
    conn = db.connect()

    run_ts = utc_now()
    run_id = run_ts.strftime('%Y%m%dT%H%M%SZ')
    run_ts_iso = run_ts.isoformat()

    params = {
        'lookback_days': cfg.lookback_days,
        'max_output_leads': cfg.max_output_leads,
        'ch_max_companies_to_check': cfg.ch_max_companies_to_check,
        'serp_cap': cfg.serp_max_calls_per_run,
    }
    db.run_start(conn, run_id, params)

    # ------------------------------------------------------------
    # 1) Sponsor Register (baseline then diff)
    # ------------------------------------------------------------
    sponsor_baselined = db.meta_get(conn, 'sponsor_baselined') or '0'
    sponsor_baselined_this_run = '0'
    sponsor_new: list[tuple[dict, dict, str]] = []  # (raw row, fields, row_key)

    sponsor_total_filtered = 0
    try:
        print('[SPONSOR] Fetching GOV.UK sponsor CSV…', flush=True)
        s_df = sponsor.fetch_df(http)
        records = s_df.to_dict(orient='records')
        filtered = _sponsor_filter(records, cfg)
        sponsor_total_filtered = len(filtered)
        print(f'[SPONSOR] Filtered to {sponsor_total_filtered} rows.', flush=True)

        if sponsor_baselined != '1':
            for row, f in filtered:
                k = sponsor.row_key(row)
                db.mark_seen(conn, k, run_ts_iso)
                db.upsert_sponsor_row(conn, k, f['name'], f['town'], f['county'], f['route'], f['subroute'], run_ts_iso)
            db.meta_set(conn, 'sponsor_baselined', '1')
            db.meta_set(conn, 'sponsor_baselined_at_utc', run_ts_iso)
            sponsor_baselined_this_run = '1'
            print('[SPONSOR] First run baseline saved. New sponsors will appear from next run.', flush=True)
        else:
            for row, f in filtered:
                k = sponsor.row_key(row)
                db.upsert_sponsor_row(conn, k, f['name'], f['town'], f['county'], f['route'], f['subroute'], run_ts_iso)
                if not db.is_seen(conn, k):
                    db.mark_seen(conn, k, run_ts_iso)
                    sponsor_new.append((row, f, k))
            print(f'[SPONSOR] New sponsor rows detected: {len(sponsor_new)}', flush=True)

    except Exception as e:
        print(f'[SPONSOR] ERROR: {e}', flush=True)

    # ------------------------------------------------------------
    # 2) Companies House: overseas-linked incorporations (PSC + officer residence)
    # ------------------------------------------------------------
    inc_to = run_ts.date().isoformat()
    inc_from = (run_ts.date() - timedelta(days=cfg.lookback_days)).isoformat()

    print(f'[CH] Pulling incorporations from {inc_from} to {inc_to}…', flush=True)
    items = ch.advanced_incorporated(http, inc_from, inc_to, size=100, max_total=cfg.ch_max_results_total)
    print(f'[CH] Pulled {len(items)} incorporations (pre-cap).', flush=True)

    items = items[:cfg.ch_max_companies_to_check]
    print(f'[CH] Processing {len(items)} incorporations (cap={cfg.ch_max_companies_to_check}).', flush=True)

    ch_candidates: list[dict] = []
    for idx, it in enumerate(items, start=1):
        if idx == 1 or idx % 10 == 0:
            print(f'[CH] Progress {idx}/{len(items)}', flush=True)

        company_number = it.get('company_number') or ''
        company_name = clean_display_name(it.get('company_name') or '')
        if not company_number or not company_name:
            continue

        seen_key = f'CH::{company_number}'
        if db.is_seen(conn, seen_key):
            continue

        # prefilter: only spend heavy calls on likely subsidiary names OR very recent
        incorporated = it.get('date_of_creation') or ''
        if not looks_like_subsidiary_name(company_name):
            # allow very recent incorporations through
            # if no date_of_creation, keep it
            pass

        profile = {}
        try:
            profile = ch.company_profile(http, company_number, timeout=cfg.ch_search_timeout)
        except Exception:
            profile = {}

        ro = (profile.get('registered_office_address') or it.get('registered_office_address') or {})
        reg_address, reg_postcode, reg_town, reg_country = ch.flatten_reg_address(ro)
        status = profile.get('company_status') or it.get('company_status') or ''
        incorporated = profile.get('date_of_creation') or it.get('date_of_creation') or ''
        sic_codes = ch.sic_codes_from_profile(profile)

        officers = []
        try:
            officers = ch.company_officers(http, company_number, timeout=cfg.ch_officers_timeout)
        except Exception:
            officers = []

        psc_items = []
        try:
            psc_items = ch.company_psc(http, company_number, timeout=cfg.ch_officers_timeout)
        except Exception:
            psc_items = []

        off_sig = _officer_signals(officers, cfg)
        psc_sig = _psc_signals(psc_items, cfg)

        # Decide whether it is a candidate at all
        overseas_flag = False
        if psc_sig['has_foreign_psc']:
            overseas_flag = True
        if off_sig['foreign_officer_residence'] >= 1:
            overseas_flag = True
        if off_sig['foreign_officer_nationality'] >= 1 and off_sig['foreign_officer_address'] >= 1:
            overseas_flag = True

        # fallback: registered office country non-UK
        if reg_country and not is_uk_country(reg_country):
            overseas_flag = True

        if not overseas_flag:
            continue

        # store company snapshot
        db.upsert_company(
            conn,
            company_number=company_number,
            name=company_name,
            incorporated=incorporated,
            status=status,
            sic_codes=sic_codes,
            reg_address=reg_address,
            reg_postcode=reg_postcode,
            reg_town=reg_town,
            reg_country=reg_country,
            last_refreshed_utc=run_ts_iso,
        )

        ch_candidates.append({
            'source': 'COMPANIES_HOUSE',
            'company_number': company_number,
            'company_name': company_name,
            'incorporated': incorporated,
            'status': status,
            'sic_codes': sic_codes,
            'reg_address': reg_address,
            'reg_postcode': reg_postcode,
            'reg_town': reg_town,
            'reg_country': reg_country,
            'signals': {**off_sig, **psc_sig},
        })

        # mark seen for CH candidate now (so next run doesn't repeat)
        db.mark_seen(conn, seen_key, run_ts_iso)

    print(f'[CH] Overseas-linked candidates: {len(ch_candidates)}', flush=True)

    # ------------------------------------------------------------
    # 3) Build sponsor leads and attempt CH matching (persist mapping)
    # ------------------------------------------------------------
    sponsor_leads: list[dict] = []

    for _row, f, row_key in sponsor_new:
        mapped = db.get_sponsor_mapping(conn, row_key)
        company_number = ''
        match_score = 0

        if mapped:
            company_number, match_score = mapped
        else:
            company_number, match_score = best_ch_match_for_sponsor(http, f['name'], f['town'], timeout=cfg.ch_search_timeout)
            if company_number and match_score >= cfg.sponsor_match_min_score:
                db.set_sponsor_mapping(conn, row_key, company_number, match_score, run_ts_iso)

        lead = {
            'source': 'SPONSOR_REGISTER',
            'company_number': company_number,
            'company_name': f['name'],
            'sponsor_route': f['route'],
            'sponsor_subroute': f['subroute'],
            'sponsor_town': f['town'],
            'match_score': match_score,
        }

        if company_number:
            # pull profile for address/postcode enrichment (cheap and useful)
            try:
                profile = ch.company_profile(http, company_number, timeout=cfg.ch_search_timeout)
            except Exception:
                profile = {}

            ro = profile.get('registered_office_address') or {}
            reg_address, reg_postcode, reg_town, reg_country = ch.flatten_reg_address(ro)
            sic_codes = ch.sic_codes_from_profile(profile)
            incorporated = profile.get('date_of_creation') or ''
            status = profile.get('company_status') or ''

            db.upsert_company(
                conn,
                company_number=company_number,
                name=clean_display_name(profile.get('company_name') or f['name']),
                incorporated=incorporated,
                status=status,
                sic_codes=sic_codes,
                reg_address=reg_address,
                reg_postcode=reg_postcode,
                reg_town=reg_town,
                reg_country=reg_country,
                last_refreshed_utc=run_ts_iso,
            )

            lead.update({
                'company_name': clean_display_name(profile.get('company_name') or f['name']),
                'incorporated': incorporated,
                'status': status,
                'sic_codes': sic_codes,
                'reg_address': reg_address,
                'reg_postcode': reg_postcode,
                'reg_town': reg_town,
                'reg_country': reg_country,
            })

        sponsor_leads.append(lead)

    # ------------------------------------------------------------
    # 4) Merge leads by company_number (keep strongest signals)
    # ------------------------------------------------------------
    merged: dict[str, dict] = {}

    def upsert_lead(ld: dict):
        key = ld.get('company_number') or ('NAME::' + norm_upper(ld.get('company_name','')))
        if key not in merged:
            merged[key] = ld
            return
        # merge sources
        existing = merged[key]
        existing_sources = set((existing.get('source') or '').split('+'))
        existing_sources.add(ld.get('source',''))
        existing['source'] = '+'.join(sorted(s for s in existing_sources if s))
        # preserve sponsor_route if present
        if not existing.get('sponsor_route') and ld.get('sponsor_route'):
            existing['sponsor_route'] = ld.get('sponsor_route')
        # merge signals if present
        if ld.get('signals'):
            exs = existing.get('signals') or {}
            exs.update(ld.get('signals'))
            existing['signals'] = exs
        # prefer richer company fields
        for k in ['company_number','company_name','incorporated','status','sic_codes','reg_address','reg_postcode','reg_town','reg_country']:
            if not existing.get(k) and ld.get(k):
                existing[k] = ld.get(k)

    for ld in sponsor_leads:
        upsert_lead(ld)
    for cd in ch_candidates:
        upsert_lead(cd)

    leads = list(merged.values())

    # ------------------------------------------------------------
    # 5) Scoring (pre-enrichment) + store signals
    # ------------------------------------------------------------
    for ld in leads:
        sig = ld.get('signals') or {}
        sr = compute_score(
            incorporated=ld.get('incorporated',''),
            sponsor_route=ld.get('sponsor_route',''),
            sic_codes=ld.get('sic_codes',''),
            has_foreign_psc=bool(sig.get('has_foreign_psc', False)),
            foreign_officer_residence=int(sig.get('foreign_officer_residence', 0) or 0),
            foreign_officer_nationality=int(sig.get('foreign_officer_nationality', 0) or 0),
            name=ld.get('company_name',''),
            website_level=ld.get('website_level',''),
            website_score=ld.get('website_score', None),
        )
        ld['score'] = sr.score
        ld['bucket'] = sr.bucket
        ld['why'] = sr.why
        ld['visa_hint'] = visa_hint('SPONSOR_REGISTER' if 'SPONSOR_REGISTER' in (ld.get('source','')) else 'COMPANIES_HOUSE', ld.get('sponsor_route',''), sr.score)

        # persist signals (only if company_number known)
        cn = ld.get('company_number')
        if cn and sig:
            if sig.get('has_foreign_psc'):
                db.add_signal(conn, run_id, cn, 'foreign_corporate_psc', 25, {
                    'psc_countries': sig.get('psc_countries', []),
                    'psc_corporates': sig.get('psc_corporates', [])
                })
            if int(sig.get('foreign_officer_residence', 0) or 0) >= 1:
                db.add_signal(conn, run_id, cn, 'foreign_officer_residence', 15, {
                    'countries': sig.get('residence_countries', [])
                })
            if int(sig.get('foreign_officer_nationality', 0) or 0) >= 1:
                db.add_signal(conn, run_id, cn, 'foreign_officer_nationality', 10, {
                    'nationalities': sig.get('nationalities', [])
                })

    # Sort + cap
    leads.sort(key=lambda x: int(x.get('score', 0)), reverse=True)
    leads = leads[:cfg.max_output_leads]

    # ------------------------------------------------------------
    # 6) Enrichment (cached; focus on stronger leads)
    # ------------------------------------------------------------
    serp_budget = {'calls': 0, 'cap': cfg.serp_max_calls_per_run}
    verified_sites = 0

    def should_enrich(ld: dict) -> bool:
        # enrich Hot + Medium, and sponsor-based leads even if watch (because sponsor data can be valuable)
        b = ld.get('bucket','WATCH')
        if b in {'HOT','MEDIUM'}:
            return True
        return 'SPONSOR_REGISTER' in (ld.get('source','')) and (ld.get('company_number') or ld.get('company_name'))

    for ld in leads:
        if not should_enrich(ld):
            ld['enrich_status'] = ld.get('enrich_status','Skipped (low priority)')
            continue

        cn = ld.get('company_number')
        cached = db.get_company(conn, cn) if cn else None
        if cached and cached.get('last_enriched_utc'):
            try:
                last = datetime.fromisoformat(cached['last_enriched_utc'])
                if utc_now() - last < timedelta(days=cfg.enrich_cache_days):
                    ld['website'] = cached.get('website') or ''
                    ld['website_level'] = cached.get('website_level') or ''
                    ld['website_score'] = cached.get('website_score')
                    ld['emails_found'] = cached.get('emails') or ''
                    ld['phones_found'] = cached.get('phones') or ''
                    ld['enrich_status'] = 'Used cached enrichment'
                    if (ld.get('website_level') or '').upper() == 'VERIFIED':
                        verified_sites += 1
                    continue
            except Exception:
                pass

        # do enrichment
        result = enrich(
            http,
            company_name=ld.get('company_name',''),
            company_number=ld.get('company_number',''),
            reg_postcode=ld.get('reg_postcode',''),
            reg_town=ld.get('reg_town',''),
            serp_key=serp_key,
            serp_budget=serp_budget,
            sleep_s=cfg.serp_sleep_seconds,
            verify_min=cfg.verify_min_score,
        )

        ld['website'] = result.get('website','')
        ld['website_level'] = result.get('website_level','')
        ld['website_score'] = result.get('website_score', None)
        ld['verification_evidence'] = result.get('verification_evidence','')
        ld['emails_found'] = result.get('emails','')
        ld['phones_found'] = result.get('phones','')
        ld['enrich_status'] = result.get('enrich_status','')

        if (ld.get('website_level') or '').upper() == 'VERIFIED':
            verified_sites += 1

        # update company cache
        if cn:
            db.upsert_company(
                conn,
                company_number=cn,
                name=ld.get('company_name',''),
                incorporated=ld.get('incorporated',''),
                status=ld.get('status',''),
                sic_codes=ld.get('sic_codes',''),
                reg_address=ld.get('reg_address',''),
                reg_postcode=ld.get('reg_postcode',''),
                reg_town=ld.get('reg_town',''),
                reg_country=ld.get('reg_country',''),
                website=ld.get('website',''),
                website_level=ld.get('website_level',''),
                website_score=ld.get('website_score') or 0,
                emails=ld.get('emails_found',''),
                phones=ld.get('phones_found',''),
                last_enriched_utc=utc_now().isoformat(),
                last_refreshed_utc=run_ts_iso,
            )

        # re-score with website info
        sig = ld.get('signals') or {}
        sr2 = compute_score(
            incorporated=ld.get('incorporated',''),
            sponsor_route=ld.get('sponsor_route',''),
            sic_codes=ld.get('sic_codes',''),
            has_foreign_psc=bool(sig.get('has_foreign_psc', False)),
            foreign_officer_residence=int(sig.get('foreign_officer_residence', 0) or 0),
            foreign_officer_nationality=int(sig.get('foreign_officer_nationality', 0) or 0),
            name=ld.get('company_name',''),
            website_level=ld.get('website_level',''),
            website_score=ld.get('website_score', None),
        )
        ld['score'] = sr2.score
        ld['bucket'] = sr2.bucket
        ld['why'] = sr2.why
        ld['visa_hint'] = visa_hint('SPONSOR_REGISTER' if 'SPONSOR_REGISTER' in (ld.get('source','')) else 'COMPANIES_HOUSE', ld.get('sponsor_route',''), sr2.score)

    # re-sort after enrichment
    leads.sort(key=lambda x: int(x.get('score', 0)), reverse=True)

    # ------------------------------------------------------------
    # 7) Persist leads + email report
    # ------------------------------------------------------------
    for i, ld in enumerate(leads, start=1):
        lead_id = f'{run_id}::{i:03d}::{ld.get("company_number") or norm_upper(ld.get("company_name",""))[:32]}'
        ld['lead_id'] = lead_id
        ld['run_id'] = run_id
        ld['created_utc'] = run_ts_iso
        db.insert_lead(conn, {
            'lead_id': lead_id,
            'run_id': run_id,
            'company_number': ld.get('company_number',''),
            'company_name': ld.get('company_name',''),
            'source': ld.get('source',''),
            'sponsor_route': ld.get('sponsor_route',''),
            'score': int(ld.get('score',0)),
            'bucket': ld.get('bucket',''),
            'why': ld.get('why',''),
            'visa_hint': ld.get('visa_hint',''),
            'website': ld.get('website',''),
            'website_level': ld.get('website_level',''),
            'website_score': ld.get('website_score') or 0,
            'emails_found': ld.get('emails_found',''),
            'phones_found': ld.get('phones_found',''),
            'enrich_status': ld.get('enrich_status',''),
            'created_utc': run_ts_iso,
        })

    run_meta = {
        'run_time_utc': run_ts_iso,
        'lookback': f'{cfg.lookback_days} days (Companies House)',
        'new_sponsors': len(sponsor_new),
        'new_ch_candidates': len(ch_candidates),
        'verified_sites': verified_sites,
        'serp_calls': serp_budget['calls'],
        'sponsor_baselined_this_run': sponsor_baselined_this_run,
    }

    subject = f'UK Expansion Leads — {run_ts.strftime("%Y-%m-%d")} (Top {len(leads)})'
    html = html_report(run_meta, leads, cfg.max_output_leads)
    csv_bytes = leads_to_csv_bytes(leads)
    csv_filename = f'uk-expansion-leads_{run_ts.strftime("%Y%m%d")}.csv'

    send_email(subject, html, csv_bytes, csv_filename)

    db.run_finish(conn, run_id, sponsor_new=len(sponsor_new), ch_candidates=len(ch_candidates), serp_calls=serp_budget['calls'], verified_sites=verified_sites)
    conn.commit()

    print('[DONE] Email sent.', flush=True)


if __name__ == '__main__':
    main()
