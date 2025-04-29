from sec_api import QueryApi
q = QueryApi("741a3bf7f33b20169aab8a96f640617013c33ccf3b321a8ba3aff41df709f6c9")

payload = {
    "query": { "query_string": { "query": "ticker:AAPL AND formType:10-K" } },
    "size": 1
}

print(q.get_filings(payload))
