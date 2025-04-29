# build_deals_yaml.py  –  auto-discover 8-K accession numbers
import os, yaml, re
from dotenv import load_dotenv
from sec_api import QueryApi, ExtractorApi

load_dotenv()
SEC_KEY = os.getenv("SEC_API_KEY")
if not SEC_KEY:
    raise RuntimeError("SEC_API_KEY not found")

q  = QueryApi(SEC_KEY)
ex = ExtractorApi(SEC_KEY)

# ---------- helper: historical ticker aliases ---------- #
ticker_alias = {
    "META": ["META", "FB"],      # Facebook tickers
    "GOOG": ["GOOG", "GOOGL"],   # Alphabet tickers
    "GOOGL": ["GOOG", "GOOGL"],
}

# If the buyer changed its name entirely (e.g. Square→Block), add here:
# ticker_alias["SQ"] = ["SQ", "SQ"]

# ---------- helper: expand keyword variants ---------- #
keyword_variants = {
    "DeepMind": ["DeepMind", "DeepMind Technologies", "DeepMind Technologies Ltd"],
    "Nervana":  ["Nervana", "Nervana Systems", "Nervana Systems Inc"],
    "GitHub":   ["GitHub", "GitHub, Inc."],
    "npm":      ["npm", "npm, Inc."],
    "Heroku":   ["Heroku", "Heroku, Inc."],
    "WhatsApp": ["WhatsApp"],  # already unique
    "Oculus":   ["Oculus"],
    # add more if you discover tricky legal names
}

def variants(base):
    return keyword_variants.get(base, [base])

# ---------- core search routine ---------- #
def find_accession(keyword_base, buyer_ticker):
    """Return accessionNo (str) or None."""
    for tkr in ticker_alias.get(buyer_ticker, [buyer_ticker]):
        # Pull up to 20 recent 8-Ks for that ticker
        filings = q.get_filings({
            "query": {"query_string": {"query": f'ticker:{tkr} AND formType:8-K'}},
            "size": 20,
            "sort": [{"filedAt": {"order": "desc"}}]
        })

        if not isinstance(filings, list):
            continue  # rare empty dict edge-case

        for f in filings:
            url  = f["linkToFilingDetails"]
            text = ex.get_section(url, "1", "text")  # Item 1 often lists the target
            for kw in variants(keyword_base):
                if re.search(re.escape(kw), text, re.I):
                    return f["accessionNo"]   # bingo
    return None

# ---------- deal list (shortened for demo) ---------- #
deals = [
    {"company": "WhatsApp",  "keyword": "WhatsApp",  "ticker": "META",
     "article": "https://www.npr.org/..."},
    {"company": "Oculus",    "keyword": "Oculus",    "ticker": "META",
     "article": "https://techcrunch.com/..."},
    {"company": "Instagram", "keyword": "Instagram", "ticker": "META",
     "article": "https://www.nytimes.com/..."},
    {"company": "Figma",     "keyword": "Figma",     "ticker": "ADBE",
     "article": "https://www.figma.com/..."},
    {"company": "DeepMind",  "keyword": "DeepMind",  "ticker": "GOOG",
     "article": "https://www.theverge.com/..."},
    # … keep the rest …
]

# ---------- main loop ---------- #
for d in deals:
    if d["ticker"].upper() == "PRIVATE":
        d["accession"] = None
        continue

    acc = find_accession(d["keyword"], d["ticker"])
    if acc:
        print(f"✓ {d['company']}: {acc}")
    else:
        print(f"⚠️  No 8-K match for {d['company']}")
    d["accession"] = acc

yaml.safe_dump(deals, open("deals.yaml", "w"))
print("deals.yaml written.")
