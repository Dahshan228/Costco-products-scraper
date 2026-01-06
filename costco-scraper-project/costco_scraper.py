#!/usr/bin/env python3
# costco_scraper.py
# Modular Costco Warehouse Scraper
# Integrates warehouse selection with robust API scraping (Search + GraphQL).

import json
import re
import requests
import csv
import sys
import time
import os
import argparse
import unicodedata
import string
import logging
import asyncio
import pathlib
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from datetime import datetime

# Try to import external dependencies
try:
    import pandas as pd
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    from playwright.async_api import async_playwright
except ImportError as e:
    print(f"Error: Missing dependency {e}. Please install: pip install pandas tenacity playwright requests")
    print("Then run: playwright install")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Configuration ---
COOKIES_FILE = pathlib.Path("costco_cookies.json")
RAW_DIR = pathlib.Path("raw_responses")
PAGE_ROWS = 200  # Number of items per page in search API
TIMEOUT = 10
X_API_KEY = "273db6be-f015-4de7-b0d6-dd4746ccd5c3"

ECOM_GRAPHQL = "https://ecom-api.costco.com/ebusiness/product/v1/products/graphql"
ECOM_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.costco.com",
    "Referer": "https://www.costco.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "client-identifier": "4900eb1f-0c10-4bd9-99c3-c59e6c1ecebf",
    "costco.env": "ecom",
    "costco.service": "restProduct",
    "X-Requested-With": "XMLHttpRequest"
}

# --- Global State ---
COOKIE_STRING = ""


# --- Helper Functions ---
def listify(x):
    if x is None: return []
    return x if isinstance(x, list) else [x]


def _normalize_badge_token(raw):
    if raw is None: return ""
    s = unicodedata.normalize("NFKC", str(raw))
    s = "".join(ch for ch in s if ch.isprintable())
    s = s.strip().lower()
    s = s.strip(" \t\n\r" + string.punctuation + "•·–—")
    s = re.sub(r"\s+", " ", s)
    return s


def norm(s): return _normalize_badge_token(s)


# --- URL Loading & Selection ---
def load_urls():
    urls = []
    base_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        p1 = os.path.join(base_dir, "urls_part1.json")
        with open(p1, encoding="utf-8") as f:
            urls.extend(json.load(f))
        p2 = os.path.join(base_dir, "urls_part2.json")
        with open(p2, encoding="utf-8") as f:
            urls.extend(json.load(f))
    except FileNotFoundError:
        print("Error: URL json files not found.")
        return []
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return []
    return urls


def parse_warehouse_info(url):
    match = re.search(r'-(\d+)\.html$', url)
    if not match: return None
    wh_id = match.group(1)
    slug = url.replace("https://www.costco.com/warehouse-locations/", "").replace(f"-{wh_id}.html", "")
    parts = slug.split('-')
    if len(parts) >= 2 and len(parts[-1]) == 2:
        state = parts[-1].upper()
        city_slug = "-".join(parts[:-1])
    else:
        state = "US"
        city_slug = slug
    city = city_slug.replace("-", " ").title()
    return {"id": wh_id, "name": city, "state": state, "url": url}


def get_warehouses():
    raw_urls = load_urls()
    warehouses = []
    seen_ids = set()
    for u in raw_urls:
        info = parse_warehouse_info(u)
        if info and info['id'] not in seen_ids:
            warehouses.append(info)
            seen_ids.add(info['id'])
    return warehouses


# --- Cookie Management ---
def load_cookies():
    try:
        if COOKIES_FILE.exists():
            return json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("load_cookies failed")
    return None


def save_cookies(cookies):
    try:
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    except Exception:
        logging.exception("save_cookies failed")


def cookie_header_from_list(cookies):
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies if 'name' in c and 'value' in c)


async def refresh_cookies_interactive():
    logging.info("Opening browser to refresh cookies (Headless Mode available)...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent=ECOM_HEADERS["User-Agent"]
        )
        page = await context.new_page()
        try:
            await page.goto("https://www.costco.com/", timeout=60000)
            logging.info("Visited Costco home. Waiting for cookies...")
        except Exception as e:
            logging.error(f"Error visiting page: {e}")
            pass

        start = time.time()
        while time.time() - start < 120:
            cookies = await context.cookies()
            if cookies:
                names = {c['name'] for c in cookies}
                if any(k in names for k in ("bm_s", "bm_sz", "_abck")):
                    break
            await asyncio.sleep(1)

        cookies = await context.cookies()
        await browser.close()
        save_cookies(cookies)
        return cookies


# --- API Logic ---
def build_search_url(warehouse_id, state):
    # Dynamic URL construction
    # loc param includes common warehouse types/regions + the target warehouse
    # whloc is the specific warehouse filter

    base = "https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_navigation"

    # Standard location set + target
    loc_ids = [
        "580-bd", f"{warehouse_id}-wh", "1255-3pl", "1321-wm", "1468-3pl",
        "283-wm", "561-wm", "725-wm", "731-wm", "758-wm", "759-wm",
        "847_0-cor", "847_0-cwt", "847_0-edi", "847_0-ehs", "847_0-membership",
        "847_0-mpt", "847_0-spc", "847_0-wm", "847_1-cwt", "847_1-edi",
        "847_d-fis", "847_lg_n1a-edi", "847_lux_us41-edi", "847_NA-cor",
        "847_NA-pharmacy", "847_NA-wm", "847_ss_u358-edi", "847_wp_r452-edi",
        "951-wm", "952-wm", "9847-wcs"
    ]
    loc_str = ",".join(loc_ids)

    params = {
        "expoption": "lw",
        "q": "*:*",
        "locale": "en-US",
        "start": "0",
        "expand": "false",
        "userLocation": state,
        "loc": loc_str,
        "whloc": f"{warehouse_id}-wh",
        "rows": str(PAGE_ROWS),
        # "url": "/grocery-household.html",      # Removed to include Health, Electronics, etc.
        # "fq": '{!tag=item_program_eligibility}item_program_eligibility:("ShipIt")', # Removed to include Warehouse Only non-ShipIt items
        "chdcategory": "true",
        "chdheader": "true"
    }

    return base + "?" + urlencode(params, safe=':(),')


def paginate_api(session, search_url, headers):
    parsed = urlparse(search_url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    start = int(qs.get("start", "0"))

    all_docs = []
    num_found = None

    logging.info(f"Starting pagination for {search_url}")

    while True:
        qs["start"] = str(start)
        url = urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(qs, doseq=True, safe=':(),'),
             parsed.fragment))

        try:
            r = session.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code != 200:
                logging.error(f"HTTP {r.status_code} fetching page")
                break

            obj = r.json()
            resp = obj.get("response", {})
            docs = resp.get("docs", [])

            if num_found is None:
                num_found = resp.get("numFound")
                logging.info(f"Total items found: {num_found}")

            if not docs:
                break

            all_docs.extend(docs)
            print(f"Fetched {len(all_docs)}/{num_found} items...", end='\r')

            if num_found and len(all_docs) >= int(num_found):
                break
            if len(docs) < int(qs.get("rows", PAGE_ROWS)):
                break

            start += int(qs.get("rows", PAGE_ROWS))
            time.sleep(0.1)

        except Exception as e:
            logging.error(f"Error during pagination: {e}")
            break

    print()  # Newline
    return all_docs


# --- GraphQL & Normalization ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def fetch_products_graphql(item_numbers, warehouse_number):
    headers = ECOM_HEADERS.copy()
    if COOKIE_STRING:
        headers["Cookie"] = COOKIE_STRING
    if X_API_KEY:
        headers["x-api-key"] = X_API_KEY

    vars_payload = {
        "itemNumbers": item_numbers,
        "clientId": ECOM_HEADERS.get("client-identifier"),
        "locale": ["en-us"],
        "warehouseNumber": str(warehouse_number)
    }

    query = """
    query ($itemNumbers: [String!], $clientId: String, $locale: [String], $warehouseNumber: String) {
        products(itemNumbers: $itemNumbers, clientId: $clientId, locale: $locale, warehouseNumber: $warehouseNumber) {
            catalogData {
                itemNumber itemId published locale buyable programTypes
                priceData { price listPrice }
                attributes { key value type pills identifier }
                description { shortDescription longDescription }
                fieldData { imageName }
            }
            fulfillmentData { itemNumber warehouseNumber clientId channel currency price listPrice }
        }
    }
    """

    payload = {"query": query, "variables": vars_payload}
    r = requests.post(ECOM_GRAPHQL, json=payload, headers=headers, timeout=60)

    if r.status_code != 200:
        return {}

    j = r.json()
    out = {}
    products = j.get("data", {}).get("products") or {}

    # Store result for each item found
    for cat in listify(products.get("catalogData") or []):
        itemnum = str(cat.get("itemNumber") or "")
        if itemnum: out[itemnum] = products

    return out


def determine_order_channel(payload, requested_warehouse=None):
    # Stricter Logic as requested
    warehouse_attr = False
    online_attr = False

    # 1. Check Search Attributes (from payload which might contain enriched search data or just gql)
    # The payload here is the GraphQL result. we need to scan its attributes.

    # Scan GraphQL attributes
    for cat in listify(payload.get("catalogData") or []):
        for a in listify(cat.get("attributes") or []):
            vals = [norm(a.get("key")), norm(a.get("value"))]
            if "online only" in vals: online_attr = True
            if "warehouse only" in vals: warehouse_attr = True

    # Also check child products (variants)
    child = payload.get("childData") or {}
    for cat in listify(child.get("catalogData") or []):
        for a in listify(cat.get("attributes") or []):
            vals = [norm(a.get("key")), norm(a.get("value"))]
            if "online only" in vals: online_attr = True
            if "warehouse only" in vals: warehouse_attr = True

    # 2. Check Program Types (Strong Warehouse Signal)
    # e.g. "InWarehouse", "LocationControlledInventory"
    wh_programs = {"inwarehouse", "warehouse", "locationcontrolledinventory", "warehousedelivery"}

    # Check parent
    for cat in listify(payload.get("catalogData") or []):
        pt = norm(cat.get("programTypes") or "")
        # pt is likely comma separated string or list
        if isinstance(cat.get("programTypes"), str):
            tokens = set(x.strip().lower() for x in cat.get("programTypes").split(','))
        else:
            tokens = set()

        if tokens & wh_programs:
            warehouse_attr = True

    # Check child
    for cat in listify(child.get("catalogData") or []):
        if isinstance(cat.get("programTypes"), str):
            tokens = set(x.strip().lower() for x in cat.get("programTypes").split(','))
        else:
            tokens = set()
        if tokens & wh_programs:
            warehouse_attr = True

    # 3. Check Online Program Types
    # e.g. "2DayDelivery", "Standard"
    on_programs = {"2daydelivery", "ecommerce", "shipit", "3rdpartydelivery", "standard", "businessdelivery",
                   "costcogrocery", "coldandfrozen", "googlegrocery"}

    for cat in listify(payload.get("catalogData") or []):
        if isinstance(cat.get("programTypes"), str):
            tokens = set(x.strip().lower() for x in cat.get("programTypes").split(','))
            if tokens & on_programs: online_attr = True

    # Check child online programs
    for cat in listify(child.get("catalogData") or []):
        if isinstance(cat.get("programTypes"), str):
            tokens = set(x.strip().lower() for x in cat.get("programTypes").split(','))
            if tokens & on_programs: online_attr = True

    if warehouse_attr and online_attr: return "any"
    if warehouse_attr: return "warehouse_only"
    if online_attr: return "online_only"

    return "any"


def normalize_doc(d, product_graph_map, warehouse_name, warehouse_id):
    item_number = d.get("item_number") or d.get("item_location_itemNumber") or d.get("itemNumber") or ""

    # Basic data
    row = {
        "warehouse_id": warehouse_id,
        "warehouse_name": warehouse_name,  # Added field
        "item_number": item_number,
        "name": d.get("item_product_name") or d.get("name") or "",
        "price": d.get("item_location_pricing_salePrice", d.get("minSalePrice", "")),
        "product_pic": d.get("item_collateral_primaryimage") or d.get("image") or "",
        "availability": d.get("item_location_availability", ""),
        # Removed review cols
    }

    # Enrichment
    payload = product_graph_map.get(item_number)

    # Search Doc Badge Check (Fallbacks)
    sd_online = False
    sd_warehouse = False

    # Check pills/badges in search doc
    badges = listify(d.get("item_pill_attributes") or []) + \
             listify(d.get("Warehouse_Only_attr_pill") or []) + \
             listify(d.get("Online_Only_attr_pill") or [])

    for b in badges:
        bn = norm(b)
        if "online only" in bn: sd_online = True
        if "warehouse only" in bn: sd_warehouse = True

    if payload:
        # Get Price from GraphQL if missing
        if not row["price"]:
            for pd in listify(payload.get("catalogData")):
                if pd.get("priceData"):
                    row["price"] = pd["priceData"].get("price")
                    break

        row["order_channel"] = determine_order_channel(payload, warehouse_id)

        # Override with Search Doc signals if GraphQL was "any" but Search Doc is explicit?
        # User asked for "Stricter logic", usually meaning "Trust the explicit tags".
        # If payload said "any" (no tags found), but search doc has "Online Only", we should probably respect that.
        if row["order_channel"] == "any":
            if sd_warehouse:
                row["order_channel"] = "warehouse_only"
            elif sd_online:
                row["order_channel"] = "online_only"

    else:
        # Fallback to search doc only
        if sd_warehouse:
            row["order_channel"] = "warehouse_only"
        elif sd_online:
            row["order_channel"] = "online_only"
        else:
            row["order_channel"] = "any"

    return row


def enrich_and_save(docs, warehouse_info):
    warehouse_id = warehouse_info['id']
    warehouse_name = warehouse_info['name']

    item_numbers = []
    for d in docs:
        n = d.get("item_number") or d.get("item_location_itemNumber") or d.get("itemNumber")
        if n: item_numbers.append(str(n))

    unique_items = sorted(set(item_numbers))
    logging.info(f"Enriching {len(unique_items)} unique items via GraphQL...")

    product_graph_map = {}

    # Batch GraphQL requests
    BATCH_SIZE = 400
    for i in range(0, len(unique_items), BATCH_SIZE):
        batch = unique_items[i:i + BATCH_SIZE]
        try:
            mapping = fetch_products_graphql(batch, warehouse_id)
            product_graph_map.update(mapping)
            print(f"Enriched {len(product_graph_map)} items...", end='\r')
        except Exception as e:
            logging.error(f"GraphQL batch failed: {e}")
        time.sleep(0.1)
    print()

    rows = [normalize_doc(d, product_graph_map, warehouse_name, warehouse_id) for d in docs]

    # Clean Filename: costco_scrape_[ID]_[Name]_products.csv
    # Sanitize name
    safe_name = "".join([c if c.isalnum() else "_" for c in warehouse_name])
    filename = f"costco_scrape_{warehouse_id}_{safe_name}_products.csv"

    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(df)} rows to {filename}")


# --- Main Logic ---
# --- Core Logic ---
def scrape_warehouse(target, session=None):
    global COOKIE_STRING

    print(f"Starting scrape for: {target['name']} (ID: {target['id']})")

    # 1. Cookies
    cookies = load_cookies()
    if not cookies:
        print("No cookies found. Launching browser to capture cookies (Playwright)...")
        try:
            cookies = asyncio.run(refresh_cookies_interactive())
        except Exception as e:
            print(f"Failed to capture cookies: {e}")
            return

    COOKIE_STRING = cookie_header_from_list(cookies)

    # 2. Setup Session
    if session is None:
        session = requests.Session()

    headers = {**ECOM_HEADERS, "Cookie": COOKIE_STRING}
    if X_API_KEY: headers["x-api-key"] = X_API_KEY

    search_url = build_search_url(target['id'], target['state'])

    # 3. Scrape
    docs = paginate_api(session, search_url, headers)

    if not docs:
        print("No items found. Cookie might be expired or warehouse has no query matches.")
        # Attempt refresh if running interactively or handle externally? 
        # For simplicity in GUI, we might just try one refresh automatically if we can,
        # but the original logic asked user. We will try ONE auto-refresh here.
        print("Attempting automatic cookie refresh...")
        try:
            cookies = asyncio.run(refresh_cookies_interactive())
            COOKIE_STRING = cookie_header_from_list(cookies)
            headers["Cookie"] = COOKIE_STRING
            docs = paginate_api(session, search_url, headers)
        except Exception as e:
            print(f"Cookie refresh failed: {e}")

    if docs:
        enrich_and_save(docs, target)
        print("Scrape completed successfully.")
    else:
        print("Scrape finished with 0 results.")


# --- CLI Entry Point ---
def main():
    # 1. Load Warehouses
    warehouses = get_warehouses()
    print(f"Loaded {len(warehouses)} warehouses.")

    # 2. Select Warehouse
    search = input("Enter warehouse name or ID to search: ").strip().lower()
    matches = [w for w in warehouses if search in w['name'].lower() or search == w['id']]

    if not matches:
        print("No matches found.")
        return

    print("\nMatches:")
    for i, m in enumerate(matches[:20]):
        print(f"{i}: {m['name']} ({m['state']}) - ID: {m['id']}")

    try:
        print("\nPlease type the number of the warehouse you want to scrape (e.g., 0):")
        idx_str = input("Enter selection number: ")
        target = matches[int(idx_str)]
    except:
        print("Invalid selection.")
        return

    # 3. Run Scrape
    scrape_warehouse(target)


if __name__ == "__main__":
    main()
