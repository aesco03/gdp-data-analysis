import yaml
import csv
from datetime import datetime
from pathlib import Path
from data_helpers import (
    acquisition_price,
    extract_company_metrics,
    get_employee_data,
    calculate_valuation_metrics
)

# Configuration
INPUT_YAML = "deals.yaml"
OUTPUT_CSV = "private_events.csv"
CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

def load_deals_with_fallback(yaml_path):
    """Load deals with validation and defaults"""
    entries = yaml.safe_load(open(yaml_path))
    
    for e in entries:
        # Set default fields if missing
        e.setdefault("data_type", "acquisition" if e.get("ticker") != "Private" else "funding")
        e.setdefault("last_updated", datetime.utcnow().isoformat())
        
        # Validate required fields
        if not all(k in e for k in ["company", "article_url"]):
            raise ValueError(f"Missing required fields in entry: {e.get('company')}")
    
    return entries

def process_deal(deal):
    """Enhanced processing pipeline for each deal"""
    metrics = {
        "company": deal["company"],
        "ticker": deal.get("ticker"),
        "data_type": deal["data_type"],
        "source_url": deal["article_url"]
    }
    
    try:
        # Get valuation (multiple fallbacks)
        if deal.get("accession"):
            metrics["valuation_usd"] = acquisition_price(deal["accession"])
        elif "valuation" in deal:
            metrics["valuation_usd"] = deal["valuation"]
        else:
            article_data = extract_company_metrics(deal["article_url"])
            metrics["valuation_usd"] = article_data.get("valuation")
        
        # Get employee count (multiple sources)
        if "employee_count" in deal:
            metrics["employees"] = deal["employee_count"]
        else:
            if deal.get("ticker") and deal["ticker"] != "Private":
                emp_data = get_employee_data(deal["ticker"])
                metrics["employees"] = emp_data["count"] if emp_data else None
            else:
                article_data = article_data or extract_company_metrics(deal["article_url"])
                metrics["employees"] = article_data.get("employees")
        
        # Calculate derived metrics
        if metrics.get("valuation_usd") and metrics.get("employees"):
            metrics.update(
                calculate_valuation_metrics(
                    metrics["valuation_usd"],
                    metrics["employees"]
                )
            )
        
        metrics["extraction_time"] = datetime.utcnow().isoformat()
        return metrics
    
    except Exception as e:
        print(f"Failed processing {deal['company']}: {str(e)}")
        return {
            **metrics,
            "error": str(e),
            "extraction_time": datetime.utcnow().isoformat()
        }

def write_output(results, csv_path):
    """Write results with dynamic columns"""
    fieldnames = set()
    for r in results:
        fieldnames.update(r.keys())
    
    fieldnames = sorted(fieldnames)  # Consistent column order
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

def main():
    entries = load_deals_with_fallback(INPUT_YAML)
    results = []
    
    for deal in entries:
        print(f"\nProcessing {deal['company']}...")
        result = process_deal(deal)
        results.append(result)
        
        # Print summary
        val = result.get("valuation_usd", "N/A")
        emp = result.get("employees", "N/A")
        print(f"{deal['company']:20s} | Val: ${val:,} | Emp: {emp} | Status: {'Success' if 'error' not in result else 'Failed'}")

    write_output(results, OUTPUT_CSV)
    print(f"\nOutput written to {OUTPUT_CSV} with {len(results)} records")

if __name__ == "__main__":
    main()