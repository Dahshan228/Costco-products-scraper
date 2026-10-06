#!/usr/bin/env python3
# costco_scraper.py
# Modular Costco Warehouse Scraper
# Pages through Costco's catalog search API from inside a real browser page.
#
# Costco retired the old search.costco.com API (it now answers 401) and moved
# its bot protection from Akamai to Kasada. Every request to the new search API
# carries a per-request Kasada token that Costco's own page script attaches to
# fetch(), so the scraper runs its requests inside a Playwright page instead of
# replaying cookies through `requests`.

import asyncio
import csv
import json
import logging
import os
import re
import sys

try:
    from playwright.async_api import async_playwright
except ImportError as e:
    print(f"Error: Missing dependency {e}. Please install: pip install playwright")
    print("Then run: playwright install chromium")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Configuration ---
SEARCH_API = "https://gdx-api.costco.com/catalog/search/api/v1/search"
# Any search results page works: loading it makes the site issue one search
# request, which is captured and used as the template for every page we fetch.
SEED_PAGE = "https://www.costco.com/s?keyword=milk"
PAGE_SIZE = 100       # largest page the API accepts
PAGE_DELAY = 0.5      # seconds between pages
SEED_TIMEOUT = 60     # seconds to wait for the site's own search request
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Request headers worth replaying. The Kasada ones (x-kpsdk-*) are deliberately
# absent: the page's fetch() adds fresh ones to every request.
REPLAY_HEADERS = ("content-type", "client_id", "client-identifier", "locale", "searchresultprovider")

CSV_FIELDS = ["warehouse_id", "warehouse_name", "item_number", "product_id", "name", "brand",
              "category", "price", "online_price", "availability", "order_channel", "product_pic"]

FETCH_JS = """async ([url, headers, body]) => {
    const r = await fetch(url, {method: 'POST', headers, body: JSON.stringify(body)});
    return [r.status, await r.text()];
}"""


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


# --- Request Building ---
def build_search_body(template, warehouse, offset):
    """The site's own search body, re-pointed at one warehouse's whole catalog.

    An empty query returns every product. filterBy is cleared because the site
    sends HIDE_OUT_OF_STOCK, which would drop exactly the rows that say an item
    is out of stock at this warehouse.
    """
    wh_loc = f"{warehouse['id']}-wh"
    body = dict(template)
    body.update({
        "query": "",
        "pageSize": PAGE_SIZE,
        "offset": offset,
        "orderBy": None,
        "personalizationEnabled": False,
        "warehouseId": wh_loc,
        "filterBy": [],
        "pageCategories": [],
    })
    if warehouse.get("state") and warehouse["state"] != "US":
        body["shipToState"] = warehouse["state"]
    locations = [loc for loc in template.get("deliveryLocations", []) if not loc.endswith("-wh")]
    body["deliveryLocations"] = [wh_loc] + locations
    return body


# --- Normalization ---
def _attr(product, key):
    vals = (product.get("attributes") or {}).get(key) or {}
    return vals.get("text") or vals.get("numbers") or []


def _min_price(price_range):
    if not price_range: return ""
    return price_range.get("minPrice") or ""


def normalize_result(result, inventory, warehouse):
    """One CSV row per item number (a product can carry several variants).

    price is the in-warehouse price at this warehouse; online_price is the
    price for delivery. Costco reports them separately and they differ, often
    by several dollars, so the warehouse price is never filled from the online
    one.
    """
    product = result.get("product") or {}
    inv = inventory.get(result.get("id")) or {}

    warehouse_price = _min_price(inv.get("warehousePrice"))
    online_price = _min_price(inv.get("deliveryPrice"))
    # An item that is out of stock here has no warehouse price but still has a
    # warehouse availability, and it is still a warehouse item.
    in_warehouse = bool(warehouse_price or inv.get("warehouseAvailability"))
    if in_warehouse and online_price:
        channel = "any"
    elif in_warehouse:
        channel = "warehouse_only"
    elif online_price:
        channel = "online_only"
    else:
        channel = ""

    images = product.get("images") or []
    pic = (_attr(product, "primary_image") or [""])[0] or (images[0].get("uri") if images else "")
    categories = _attr(product, "category_names")
    brands = product.get("brands") or []

    base = {
        "warehouse_id": warehouse["id"],
        "warehouse_name": warehouse["name"],
        "product_id": result.get("id", ""),
        "name": product.get("title", ""),
        "brand": brands[0] if brands else "",
        "category": categories[-1] if categories else "",
        "price": warehouse_price,
        "online_price": online_price,
        "availability": (inv.get("warehouseAvailability") or "").lower().replace("_", " "),
        "order_channel": channel,
        "product_pic": pic,
    }

    # Results without an item number are Costco Travel packages, not products.
    item_numbers = [v.get("id") for v in product.get("variants") or [] if v.get("id")]
    return [dict(base, item_number=n) for n in item_numbers]


# --- Scraping ---
async def capture_search_template(page):
    """Load a search page and capture the request the site itself sends."""
    captured = asyncio.get_running_loop().create_future()

    def on_request(request):
        if request.url.startswith(SEARCH_API) and request.method == "POST" and not captured.done():
            captured.set_result(request)

    page.on("request", on_request)
    await page.goto(SEED_PAGE, timeout=SEED_TIMEOUT * 1000)
    request = await asyncio.wait_for(captured, timeout=SEED_TIMEOUT)
    page.remove_listener("request", on_request)

    all_headers = await request.all_headers()
    headers = {k: v for k, v in all_headers.items() if k in REPLAY_HEADERS}
    return json.loads(request.post_data), headers


async def fetch_page(page, headers, body):
    status, text = await page.evaluate(FETCH_JS, [SEARCH_API, headers, body])
    if status != 200:
        raise RuntimeError(f"HTTP {status} from search API: {text[:200]}")
    return json.loads(text)


async def scrape_warehouse_async(warehouse):
    async with async_playwright() as p:
        # Kasada blocks headless Chromium, so the window has to be visible.
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()
        try:
            template, headers = await capture_search_template(page)

            rows, seen, offset, total = [], set(), 0, None
            while total is None or offset < total:
                data = await fetch_page(page, headers, build_search_body(template, warehouse, offset))
                result = data.get("searchResult") or {}
                results = result.get("results") or []
                if total is None:
                    total = int(result.get("totalSize") or 0)
                    logging.info(f"Total products found: {total}")
                if not results:
                    break

                inventory = {i.get("productId"): i for i in data.get("inventoryResponse") or []}
                for r in results:
                    for row in normalize_result(r, inventory, warehouse):
                        # The same item number can sit under several products
                        # (bundles, listing variants); keep the first.
                        if row["item_number"] in seen:
                            continue
                        seen.add(row["item_number"])
                        rows.append(row)

                offset += len(results)
                print(f"Fetched {offset}/{total} products...", end='\r')
                await asyncio.sleep(PAGE_DELAY)
            print()

            if total and offset < total:
                logging.warning(f"Stopped at {offset} of {total} products; the catalog is incomplete.")
            return rows
        finally:
            await browser.close()


def save_rows(rows, warehouse):
    safe_name = "".join([c if c.isalnum() else "_" for c in warehouse['name']])
    filename = f"costco_scrape_{warehouse['id']}_{safe_name}_products.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info(f"Saved {len(rows)} rows to {filename}")
    return filename


# --- Core Logic ---
def scrape_warehouse(target):
    print(f"Starting scrape for: {target['name']} (ID: {target['id']})")
    try:
        rows = asyncio.run(scrape_warehouse_async(target))
    except asyncio.TimeoutError:
        print("The site never sent a search request; the page may have been blocked.")
        return None
    except Exception as e:
        print(f"Scrape failed: {e}")
        return None

    if not rows:
        print("Scrape finished with 0 results.")
        return None

    filename = save_rows(rows, target)
    print("Scrape completed successfully.")
    return filename


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
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    # 3. Run Scrape
    scrape_warehouse(target)


if __name__ == "__main__":
    main()
