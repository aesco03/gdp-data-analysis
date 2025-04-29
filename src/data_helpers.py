import os, re
from functools import lru_cache
from dotenv import load_dotenv
from sec_api import QueryApi, ExtractorApi

# ── SEC config ─────────────────────────────────────────────
load_dotenv()
SEC_KEY = os.getenv("SEC_API_KEY")
if not SEC_KEY:
    raise RuntimeError("SEC_API_KEY not found in .env or shell")

q  = QueryApi(SEC_KEY)
ex = ExtractorApi(SEC_KEY)

# ── cached Item-1 text fetch ───────────────────────────────
@lru_cache(maxsize=128)
def _item1_text(accession):
    hit = q.get_filings({
        "query": {"query_string": {"query": f"accessionNumber:{accession}"}},
        "size": 1
    })
    if not hit:
        return ""
    url = hit[0]["linkToFilingDetails"]
    return ex.get_section(url, "1", "text")

# ── public-company employee count (latest 10-K) ────────────
def get_employee_count(ticker: str) -> int | None:
    """Return latest employee count from any 10-K variant."""
    hits = q.get_filings({
        "query": {"query_string": {
            # match 10-K, 10-K/A, 10-K405, etc.
            "query": f'ticker:{ticker} AND formType:10-K*'
        }},
        "size": 1,
        "sort": [{"filedAt": {"order": "desc"}}]
    })

    if not isinstance(hits, list) or len(hits) == 0:
        print(f"⚠️  SEC-API: no 10-K variant found for {ticker}")
        return None

    text = _item1_text(hits[0]["accessionNo"])
    m = re.search(r'([0-9,]+)\s+employees', text, re.I)
    return int(m.group(1).replace(',', '')) if m else None
