# Requires:
#   pip install playwright requests pandas
#   playwright install
#
#
# Notes:
# - Edit SEARCH_API_URL if you ever need to change the exact query.
# - The script will open a visible Chromium window for you to sign in when cookies need refreshing.

import asyncio, json, csv, pathlib, time, logging, sys
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import requests
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ========= CONFIG =========
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
TIMEOUT = 30
X_API_KEY = "273db6be-f015-4de7-b0d6-dd4746ccd5c3"  # keep if you captured it previously; remove if not required
# ===========================

CSV_FIELDS = [
    "id","item_number","name","price","listPrice","product_pic","product_description",
    "deliveryStatus","availability","review_count","review_ratings","categoryPath"
]

def load_cookies():
    if not COOKIES_FILE.exists():
        return None
    try:
        return json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logging.exception("Failed to read cookies file: %s", e)
        return None

def save_cookies(cookies):
    try:
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
        logging.info("Wrote cookies to %s", COOKIES_FILE)
    except Exception as e:
        logging.exception("Failed to write cookies: %s", e)

def cookie_header_from_list(cookies):
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies if 'name' in c and 'value' in c)

def normalize_doc(d):
    return {
        "id": d.get("id",""),
        "item_number": d.get("item_number", d.get("item_location_itemNumber","")),
        "name": d.get("item_product_name") or d.get("name",""),
        "price": d.get("item_location_pricing_salePrice", d.get("minSalePrice","")),
        "listPrice": d.get("item_location_pricing_listPrice",""),
        "product_pic": d.get("item_collateral_primaryimage") or d.get("image") or (d.get("images")[0].get("item_collateral_primaryimage") if isinstance(d.get("images"), list) and d.get("images") else ""),
        "product_description": d.get("description") or d.get("item_short_description") or d.get("item_product_short_description") or "",
        "deliveryStatus": d.get("deliveryStatus",""),
        "availability": d.get("item_location_availability",""),
        "review_count": d.get("item_product_review_count", d.get("item_review_count","")),
        "review_ratings": d.get("item_product_review_ratings", d.get("item_review_ratings", d.get("item_ratings",""))),
        "categoryPath": "|".join(d.get("categoryPath_ss", []))
    }

def write_snapshot_csv(docs, out_path=OUT_CSV):
    rows = [normalize_doc(d) for d in docs]
    df = pd.DataFrame(rows, columns=CSV_FIELDS)
    # write previous
    if out_path.exists():
        out_path.rename(PREV_CSV)
    df.to_csv(out_path, index=False)
    logging.info("Wrote snapshot %s (%d rows)", out_path, len(df))
    return df

def probe_api_with_cookies(cookie_header):
    # quick probe start=0 rows=1 to detect 401
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
            logging.warning("Navigation may have failed initially: %s. Continue to sign in/refresh the page in the opened browser.", e)
        # wait for user to sign in and site to make API calls
        logging.info("Please sign in (if needed) and refresh the grocery page in the opened browser. Waiting up to 120s...")
        start = time.time()
        while time.time() - start < 120:
            # check cookies periodically
            cookies = await context.cookies()
            if cookies:
                # heuristic: presence of session cookies like 'bm_s' or '_abck' likely sufficient
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
    # index by id
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
    # write CSVs
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
    # restore CSVs: new snapshot already saved by caller
    return added_path, removed_path, changed_path

def run_sync():
    # 1) check cookies and probe
    cookies = load_cookies()
    cookie_header = cookie_header_from_list(cookies) if cookies else None
    status, resp = probe_api_with_cookies(cookie_header)
    if status != 200:
        logging.info("Probe failed or unauthorized (status=%s). Will refresh cookies interactively.", status)
        # refresh via Playwright
        try:
            cookies = asyncio.run(refresh_cookies_interactive())
            cookie_header = cookie_header_from_list(cookies) if cookies else None
        except Exception as e:
            logging.exception("Interactive cookie refresh failed: %s", e)
            raise SystemExit("Cookie refresh failed; cannot proceed.")
        # probe again
        status, resp = probe_api_with_cookies(cookie_header)
        if status != 200:
            logging.error("Probe still failed after cookie refresh. Status=%s", status)
            raise SystemExit("Authorization failed after refresh; manual inspection required.")
    # 2) fetch full pages
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
    # save raw first response if we have it from probe
    if resp is not None:
        try:
            obj = resp.json()
            save_raw_response(obj)
        except Exception:
            pass
    # paginate
    try:
        docs = paginate_api(sess, SEARCH_API_URL, headers, page_rows=PAGE_ROWS)
    except RuntimeError as e:
        logging.exception("Runtime error during pagination: %s", e)
        raise
    # write snapshot CSV
    new_df = write_snapshot_csv(docs, out_path=OUT_CSV)
    # compute deltas
    compute_deltas(new_df, prev_path=PREV_CSV)

if __name__ == "__main__":
    run_sync()
