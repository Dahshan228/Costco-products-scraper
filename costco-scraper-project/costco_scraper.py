#!/usr/bin/env python3
# costco_scraper_strict_order_channel.py
# Requires:
#   pip install playwright requests pandas tenacity
#   playwright install
#
# Purpose:
# - Paginate Costco search API (uses saved cookies or opens Playwright to refresh)
# - Enrich rows by calling ecom GraphQL (passes warehouseNumber extracted from SEARCH_API_URL)
# - Force price to None when deliveryStatus is missing/empty
# - Add order_channel values: online_only | warehouse_only | both | any
# - Uses strict matching (only explicit "online only" / "warehouse only" attributes or fulfillmentData)
# - GraphQL batch size set to 100
#
# Edit SEARCH_API_URL, X_API_KEY, COOKIES_FILE, COOKIE_STRING as needed.

import asyncio
import json
import pathlib
import time
import logging
import re
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import requests
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ===== CONFIG =====
SEARCH_API_URL = (
    "https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_navigation"
    "?expoption=lw&q=*%3A*&locale=en-US&start=0&expand=false&userLocation=IL"
    "&loc=580-bd%2C388-wh%2C1255-3pl%2C1321-wm%2C1468-3pl%2C283-wm%2C561-wm%2C725-wm%2C731-wm"
    "%2C758-wm%2C759-wm%2C847_0-cor%2C847_0-cwt%2C847_0-edi%2C847_0-ehs%2C847_0-membership"
    "%2C847_0-mpt%2C847_0-spc%2C847_0-wm%2C847_1-cwt%2C847_1-edi%2C847_d-fis%2C847_lg_n1a-edi"
    "%2C847_lux_us41-edi%2C847_NA-cor%2C847_NA-pharmacy%2C847_NA-wm%2C847_ss_u358-edi"
    "%2C847_wp_r452-edi%2C951-wm%2C952-wm%2C9847-wcs&whloc=388-wh&rows=24&url=%2Fgrocery-household.html"
    "&fq=%7B%21tag%3Ditem_program_eligibility%7Ditem_program_eligibility%3A(%22ShipIt%22)"
    "&chdcategory=true&chdheader=true"
)
COOKIES_FILE = pathlib.Path("costco_cookies.json")
RAW_DIR = pathlib.Path("raw_responses")
OUT_CSV = pathlib.Path("grocery_all_products.csv")
PREV_CSV = pathlib.Path("grocery_all_products.prev.csv")
PAGE_ROWS = 100
TIMEOUT = 5
X_API_KEY = "273db6be-f015-4de7-b0d6-dd4746ccd5c3"  # optional; remove if not needed

ECOM_GRAPHQL = "https://ecom-api.costco.com/ebusiness/product/v1/products/graphql"
ECOM_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.costco.com",
    "Referer": "https://www.costco.com/",
    "User-Agent": "Mozilla/5.0",
    "client-identifier": "4900eb1f-0c10-4bd9-99c3-c59e6c1ecebf",
    "costco.env": "ecom",
    "costco.service": "restProduct",
}
COOKIE_STRING = ""  # optional cookie string for GraphQL if required

# derive warehouse number from SEARCH_API_URL (e.g., whloc=388-wh -> 388)
def extract_warehouse_number_from_search_url(url):
    parsed = urlparse(url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    candidates = [qs.get("whloc"), qs.get("wh"), qs.get("warehouse"), qs.get("whlocs")]
    for c in candidates:
        if not c:
            continue
        for part in re.split(r"[,\s;]+", str(c)):
            m = re.match(r"(\d+)", part)
            if m:
                return m.group(1)
    return None

WAREHOUSE_NUMBER = extract_warehouse_number_from_search_url(SEARCH_API_URL) or "847"
GRAPHQL_BATCH = 100
GRAPHQL_BATCH_SLEEP = 0.5  # pause between batches to reduce throttling

CSV_FIELDS = [
    "id","item_number","name","price","listPrice","product_pic","product_description",
    "deliveryStatus","availability","review_count","review_ratings","categoryPath",
    "order_channel"
]
# ==================

# ---------------- helpers ----------------
def load_cookies():
    if not COOKIES_FILE.exists():
        return None
    try:
        return json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None

def save_cookies(cookies):
    try:
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    except Exception:
        pass

def cookie_header_from_list(cookies):
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies if 'name' in c and 'value' in c)

def listify(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

# ---------------- pagination / cookie refresh ----------------
def probe_api_with_cookies(cookie_header):
    parsed = urlparse(SEARCH_API_URL)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    qs["start"] = "0"
    qs["rows"] = "1"
    new_q = urlencode(qs, doseq=True)
    req_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_q, parsed.fragment))
    headers = {
        "Accept":"application/json",
        "User-Agent":"Mozilla/5.0",
        "Referer":"https://www.costco.com/",
        "X-Requested-With":"XMLHttpRequest"
    }
    if X_API_KEY:
        headers["x-api-key"] = X_API_KEY
    if cookie_header:
        headers["Cookie"] = cookie_header
    try:
        r = requests.get(req_url, headers=headers, timeout=TIMEOUT)
        return r.status_code, r
    except Exception as e:
        logging.exception("Probe request failed: %s", e)
        return None, None

async def refresh_cookies_interactive():
    logging.info("Refreshing cookies via Playwright headful Chromium. A browser window will open; please sign in if required and wait until the grocery page finishes.")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        try:
            await page.goto("https://www.costco.com/grocery-household.html", timeout=60000)
        except Exception as e:
            logging.warning("Initial navigation issue: %s", e)
        logging.info("Please sign in (if needed) and refresh the grocery page in the opened browser. Waiting up to 120s...")
        start = time.time()
        while time.time() - start < 120:
            cookies = await context.cookies()
            if cookies:
                names = {c['name'] for c in cookies}
                if any(k in names for k in ("bm_s","bm_sz","bm_sc","_abck","ak_bmsc")):
                    logging.info("Detected security/auth cookies: %s", ", ".join(sorted(names)))
                    break
            await asyncio.sleep(2)
        cookies = await context.cookies()
        await browser.close()
        save_cookies(cookies)
        return cookies

def paginate_api(session, base_url, headers, page_rows=PAGE_ROWS):
    parsed = urlparse(base_url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    qs["rows"] = str(page_rows)
    start = int(qs.get("start","0"))
    all_docs = []
    num_found = None
    while True:
        qs["start"] = str(start)
        new_q = urlencode(qs, doseq=True)
        req_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_q, parsed.fragment))
        logging.info("Fetching: %s", req_url)
        r = session.get(req_url, headers=headers, timeout=TIMEOUT)
        logging.info("Status %s", r.status_code)
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized during pagination")
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        obj = r.json()
        response = obj.get("response", {})
        docs = response.get("docs", [])
        if num_found is None:
            num_found = response.get("numFound")
            logging.info("numFound: %s", num_found)
        if not docs:
            break
        all_docs.extend(docs)
        logging.info("Collected %d / %s", len(all_docs), num_found)
        if num_found is not None and len(all_docs) >= int(num_found):
            break
        if len(docs) < page_rows:
            break
        start += page_rows
        time.sleep(0.2)
    return all_docs

def save_raw_response(obj):
    RAW_DIR.mkdir(exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    p = RAW_DIR / f"search_response_{ts}.json"
    p.write_text(json.dumps(obj, indent=2))
    logging.info("Saved raw response to %s", p)
    return p

# ---------------- GraphQL enrichment ----------------
def determine_order_channel_from_catalog_payload(payload):
    """
    Strict detection:
    - scan catalogData.attributes and childData.catalogData.attributes for explicit phrases:
        'online only' -> online_only
        'warehouse only' -> warehouse_only
    - detect warehouse availability from fulfillmentData (warehouseNumber present)
    - return 'both' if explicit online-only AND warehouse fulfillment present
    - otherwise 'any'
    """
    online_attr = False
    warehouse_attr = False
    warehouse_fulfill = False

    def scan_attrs(attrs):
        nonlocal online_attr, warehouse_attr
        for a in listify(attrs):
            try:
                key = str(a.get("key") or "").strip().lower()
                val = str(a.get("value") or "").strip().lower()
            except Exception:
                continue
            if "online only" in key or "online only" in val:
                online_attr = True
            if "warehouse only" in key or "warehouse only" in val:
                warehouse_attr = True

    # catalogData
    catalog_list = payload.get("catalogData") or []
    for cat in listify(catalog_list):
        scan_attrs(cat.get("attributes") or [])

    # childData.catalogData
    child = payload.get("childData") or {}
    child_catalog = child.get("catalogData") or []
    for cc in listify(child_catalog):
        scan_attrs(cc.get("attributes") or [])

    # fulfillmentData -> warehouse presence
    fulldata = payload.get("fulfillmentData") or []
    for f in listify(fulldata):
        if f.get("warehouseNumber"):
            warehouse_fulfill = True
        ch = str(f.get("channel") or "").strip().lower()
        if "warehouse" in ch or "store" in ch or "instore" in ch:
            warehouse_fulfill = True

    if online_attr and (warehouse_attr or warehouse_fulfill):
        return "both"
    if online_attr and not (warehouse_attr or warehouse_fulfill):
        return "online_only"
    if (warehouse_attr or warehouse_fulfill) and not online_attr:
        return "warehouse_only"
    return "any"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def fetch_products_graphql(item_numbers, warehouse_number):
    """
    item_numbers: list[str]; warehouse_number: required string (e.g., "388")
    Returns dict itemNumber->products_payload
    """
    headers = ECOM_HEADERS.copy()
    headers["Content-Type"] = "application/json"
    if COOKIE_STRING:
        headers["Cookie"] = COOKIE_STRING

    vars_payload = {
        "itemNumbers": item_numbers,
        "clientId": ECOM_HEADERS.get("client-identifier") or "4900eb1f-0c10-4bd9-99c3-c59e6c1ecebf",
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
                additionalFieldData { rating numberOfRating }
                fieldData { imageName }
            }
            fulfillmentData { itemNumber warehouseNumber clientId channel currency price listPrice }
            childData {
                catalogData { itemNumber buyable published parentId programTypes priceData { price listPrice } attributes { key value } }
                fulfillmentData { itemNumber warehouseNumber channel price }
            }
        }
    }
    """

    payload = {"query": query, "variables": vars_payload}
    r = requests.post(ECOM_GRAPHQL, json=payload, headers=headers, timeout=60)

    if r.status_code != 200:
        logging.error("GraphQL fetch failed: status=%s body=%s", r.status_code, r.text[:2000])
        r.raise_for_status()

    j = r.json()
    if "errors" in j:
        logging.error("GraphQL returned errors: %s", j.get("errors"))
        return {}

    out = {}
    products = j.get("data", {}).get("products") or {}
    top_catalog = products.get("catalogData") or []
    for cat in listify(top_catalog):
        itemnum = str(cat.get("itemNumber") or "")
        if itemnum:
            out[itemnum] = products
    child = products.get("childData") or {}
    child_catalog = child.get("catalogData") or []
    for cc in listify(child_catalog):
        itemnum = str(cc.get("itemNumber") or "")
        if itemnum and itemnum not in out:
            out[itemnum] = products
    fulldata = products.get("fulfillmentData") or []
    for f in listify(fulldata):
        itemnum = str(f.get("itemNumber") or "")
        if itemnum and itemnum not in out:
            out[itemnum] = products
    return out

# ---------------- normalize + enrichment ----------------
def normalize_doc_with_enrichment(d, product_graph_map):
    item_number = d.get("item_number") or d.get("item_location_itemNumber") or d.get("itemNumber") or ""
    name = d.get("item_product_name") or d.get("name") or ""
    description = ""
    if isinstance(d.get("description"), dict):
        description = d["description"].get("shortDescription", "")
    else:
        description = d.get("description") or d.get("item_short_description") or d.get("item_product_short_description", "")

    product_pic = d.get("item_collateral_primaryimage") or d.get("image") or ""
    if not product_pic and isinstance(d.get("images"), list) and d.get("images"):
        first = d["images"][0]
        if isinstance(first, dict):
            product_pic = first.get("item_collateral_primaryimage") or first.get("image") or ""

    price_val = d.get("item_location_pricing_salePrice", d.get("minSalePrice", None))
    if price_val == "":
        price_val = None

    row = {
        "id": d.get("id", ""),
        "item_number": item_number,
        "name": name,
        "price": price_val,
        "listPrice": d.get("item_location_pricing_listPrice", ""),
        "product_pic": product_pic,
        "product_description": description,
        "deliveryStatus": d.get("deliveryStatus", ""),
        "availability": d.get("item_location_availability", ""),
        "review_count": d.get("item_product_review_count", d.get("item_review_count", "")),
        "review_ratings": d.get("item_product_review_ratings", d.get("item_review_ratings", d.get("item_ratings", ""))),
        "categoryPath": "|".join(d.get("categoryPath_ss", [])) if isinstance(d.get("categoryPath_ss"), list) else (d.get("categoryPath_ss", "") or "")
    }

    ds = row.get("deliveryStatus")
    if ds is None or (isinstance(ds, str) and ds.strip() == ""):
        row["price"] = None

    order_channel = "any"
    if item_number and item_number in product_graph_map:
        payload = product_graph_map[item_number]
        try:
            order_channel = determine_order_channel_from_catalog_payload(payload)
        except Exception:
            order_channel = "any"
    else:
        ds_low = (row.get("deliveryStatus") or "").lower()
        if "online only" in ds_low:
            order_channel = "online_only"
        elif "warehouse only" in ds_low:
            order_channel = "warehouse_only"
        else:
            order_channel = "any"

    row["order_channel"] = order_channel
    return row

def write_snapshot_csv_enriched(docs):
    item_numbers = []
    for d in docs:
        n = d.get("item_number") or d.get("item_location_itemNumber") or d.get("itemNumber") or ""
        if n:
            item_numbers.append(str(n))
    unique_item_numbers = sorted(set(item_numbers))
    logging.info("Unique item numbers to enrich: %d", len(unique_item_numbers))

    product_graph_map = {}
    BATCH = GRAPHQL_BATCH
    for i in range(0, len(unique_item_numbers), BATCH):
        batch = unique_item_numbers[i:i+BATCH]
        try:
            mapping = fetch_products_graphql(batch, warehouse_number=WAREHOUSE_NUMBER)
            product_graph_map.update(mapping)
            logging.info("Fetched %d products from GraphQL", len(mapping))
        except Exception as e:
            logging.exception("Product GraphQL fetch failed for batch: %s", e)
        time.sleep(GRAPHQL_BATCH_SLEEP)

    rows = [normalize_doc_with_enrichment(d, product_graph_map) for d in docs]
    df = pd.DataFrame(rows, columns=CSV_FIELDS)
    if OUT_CSV.exists():
        OUT_CSV.rename(PREV_CSV)
    df.to_csv(OUT_CSV, index=False)
    logging.info("Wrote enriched snapshot %s (%d rows)", OUT_CSV, len(df))
    return df

# ---------------- deltas ----------------
def compute_deltas(new_df, prev_path=PREV_CSV):
    added_path = pathlib.Path("added.csv")
    removed_path = pathlib.Path("removed.csv")
    changed_path = pathlib.Path("changed.csv")
    if not prev_path.exists():
        logging.info("No previous snapshot to diff against; writing full snapshot only.")
        for p in (added_path, removed_path, changed_path):
            if p.exists():
                p.unlink()
        new_df.to_csv(OUT_CSV, index=False)
        return added_path, removed_path, changed_path
    prev_df = pd.read_csv(prev_path, dtype=str).fillna("")
    prev_df.set_index("id", inplace=True)
    new_df = new_df.fillna("")
    new_df.set_index("id", inplace=True)
    prev_ids = set(prev_df.index)
    new_ids = set(new_df.index)
    added_ids = sorted(new_ids - prev_ids)
    removed_ids = sorted(prev_ids - new_ids)
    common = sorted(prev_ids & new_ids)
    changed_rows = []
    for pid in common:
        prev_row = prev_df.loc[pid].to_dict()
        new_row = new_df.loc[pid].to_dict()
        diffs = {}
        for col in CSV_FIELDS:
            pv = str(prev_row.get(col, ""))
            nv = str(new_row.get(col, ""))
            if pv != nv:
                diffs[col] = {"old": pv, "new": nv}
        if diffs:
            entry = {"id": pid, "diffs": json.dumps(diffs), "name": new_row.get("name","")}
            changed_rows.append(entry)
    if added_ids:
        pd.DataFrame([new_df.loc[i].to_dict() for i in added_ids]).to_csv(added_path, index=False)
    else:
        if added_path.exists(): added_path.unlink()
    if removed_ids:
        pd.DataFrame([prev_df.loc[i].to_dict() for i in removed_ids]).to_csv(removed_path, index=False)
    else:
        if removed_path.exists(): removed_path.unlink()
    if changed_rows:
        pd.DataFrame(changed_rows).to_csv(changed_path, index=False)
    else:
        if changed_path.exists(): changed_path.unlink()
    logging.info("Delta results: added=%d removed=%d changed=%d", len(added_ids), len(removed_ids), len(changed_rows))
    return added_path, removed_path, changed_path

# ---------------- main flow ----------------
def run_sync():
    cookies = load_cookies()
    cookie_header = cookie_header_from_list(cookies) if cookies else None
    status, resp = probe_api_with_cookies(cookie_header)
    if status != 200:
        logging.info("Probe failed or unauthorized (status=%s). Will refresh cookies interactively.", status)
        try:
            cookies = asyncio.run(refresh_cookies_interactive())
            cookie_header = cookie_header_from_list(cookies) if cookies else None
        except Exception as e:
            logging.exception("Interactive cookie refresh failed: %s", e)
            raise SystemExit("Cookie refresh failed; cannot proceed.")
        status, resp = probe_api_with_cookies(cookie_header)
        if status != 200:
            logging.error("Probe still failed after cookie refresh. Status=%s", status)
            raise SystemExit("Authorization failed after refresh; manual inspection required.")
    headers = {
        "Accept":"application/json",
        "User-Agent":"Mozilla/5.0",
        "Referer":"https://www.costco.com/",
        "X-Requested-With":"XMLHttpRequest"
    }
    if X_API_KEY:
        headers["x-api-key"] = X_API_KEY
    if cookie_header:
        headers["Cookie"] = cookie_header
    sess = requests.Session()
    sess.headers.update({"User-Agent": headers.get("User-Agent")})
    if resp is not None:
        try:
            obj = resp.json()
            save_raw_response(obj)
        except Exception:
            pass
    try:
        docs = paginate_api(sess, SEARCH_API_URL, headers, page_rows=PAGE_ROWS)
    except RuntimeError as e:
        logging.exception("Runtime error during pagination: %s", e)
        raise
    new_df = write_snapshot_csv_enriched(docs)
    compute_deltas(new_df, prev_path=PREV_CSV)

if __name__ == "__main__":
    run_sync()
