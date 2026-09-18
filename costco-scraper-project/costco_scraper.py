#!/usr/bin/env python3
"""Costco warehouse scraper with CLI and reusable scraping APIs."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import pathlib
import re
import string
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

try:
    from playwright.async_api import async_playwright
except ImportError:  # pragma: no cover - graceful runtime guard
    async_playwright = None


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
LOGGER = logging.getLogger("costco_scraper")


class ScraperError(RuntimeError):
    """Raised when scraper operations fail in a recoverable way."""


RETRIABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}


DEFAULT_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://www.costco.com",
    "Referer": "https://www.costco.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "client-identifier": "4900eb1f-0c10-4bd9-99c3-c59e6c1ecebf",
    "costco.env": "ecom",
    "costco.service": "restProduct",
    "X-Requested-With": "XMLHttpRequest",
}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        LOGGER.warning("Invalid %s=%r, using default %s", name, raw, default)
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        LOGGER.warning("Invalid %s=%r, using default %s", name, raw, default)
        return default
    return max(minimum, value)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class ScraperConfig:
    page_rows: int = 200
    timeout: int = 15
    max_retries: int = 4
    retry_backoff_base: float = 1.0
    min_request_interval: float = 0.1
    graphql_batch_size: int = 200
    cookie_capture_wait_seconds: int = 120
    cookie_capture_headless: bool = False
    output_dir: pathlib.Path = field(default_factory=lambda: SCRIPT_DIR)
    cookies_file: pathlib.Path = field(default_factory=lambda: SCRIPT_DIR / "costco_cookies.json")
    x_api_key: str | None = None
    headers: dict[str, str] = field(default_factory=lambda: DEFAULT_HEADERS.copy())
    graphql_url: str = "https://ecom-api.costco.com/ebusiness/product/v1/products/graphql"

    @classmethod
    def from_env(
        cls,
        *,
        output_dir: str | os.PathLike[str] | None = None,
        log_level: str | None = None,
    ) -> ScraperConfig:
        chosen_output = (
            pathlib.Path(output_dir)
            if output_dir
            else pathlib.Path(os.getenv("COSTCO_OUTPUT_DIR", SCRIPT_DIR))
        )
        chosen_output = chosen_output.expanduser().resolve()
        cookies_file_env = os.getenv("COSTCO_COOKIES_FILE")
        cookies_file = (
            pathlib.Path(cookies_file_env).expanduser().resolve()
            if cookies_file_env
            else chosen_output / "costco_cookies.json"
        )

        headers = DEFAULT_HEADERS.copy()
        client_identifier = os.getenv("COSTCO_CLIENT_IDENTIFIER")
        if client_identifier:
            headers["client-identifier"] = client_identifier

        configured_log = (log_level or os.getenv("COSTCO_LOG_LEVEL", "INFO")).upper()
        logging.basicConfig(
            level=getattr(logging, configured_log, logging.INFO),
            format="%(asctime)s %(levelname)s %(message)s",
        )

        return cls(
            page_rows=_env_int("COSTCO_PAGE_ROWS", 200),
            timeout=_env_int("COSTCO_TIMEOUT", 15),
            max_retries=_env_int("COSTCO_MAX_RETRIES", 4),
            retry_backoff_base=_env_float("COSTCO_RETRY_BACKOFF_BASE", 1.0, minimum=0.1),
            min_request_interval=_env_float("COSTCO_MIN_REQUEST_INTERVAL", 0.1, minimum=0.0),
            graphql_batch_size=_env_int("COSTCO_GRAPHQL_BATCH_SIZE", 200),
            cookie_capture_wait_seconds=_env_int("COSTCO_COOKIE_CAPTURE_WAIT_SECONDS", 120),
            cookie_capture_headless=_env_bool("COSTCO_COOKIE_CAPTURE_HEADLESS", False),
            output_dir=chosen_output,
            cookies_file=cookies_file,
            x_api_key=os.getenv("COSTCO_X_API_KEY") or None,
            headers=headers,
        )


def listify(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _normalize_badge_token(raw: Any) -> str:
    if raw is None:
        return ""
    text = unicodedata.normalize("NFKC", str(raw))
    text = "".join(ch for ch in text if ch.isprintable())
    text = text.strip().lower()
    text = text.strip(" \t\n\r" + string.punctuation + "•·–—")
    text = re.sub(r"\s+", " ", text)
    return text


def norm(text: Any) -> str:
    return _normalize_badge_token(text)


def load_urls(base_dir: pathlib.Path = SCRIPT_DIR) -> list[str]:
    urls: list[str] = []
    files = ["urls_part1.json", "urls_part2.json"]
    for filename in files:
        path = base_dir / filename
        if not path.exists():
            raise ScraperError(f"URL file not found: {path}")
        try:
            with path.open(encoding="utf-8") as handle:
                urls.extend(json.load(handle))
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Invalid JSON in {path}: {exc}") from exc
    return urls


def parse_warehouse_info(url: str) -> dict[str, str] | None:
    match = re.search(r"-(\d+)\.html$", url)
    if not match:
        return None

    warehouse_id = match.group(1)
    slug = url.replace("https://www.costco.com/warehouse-locations/", "").replace(f"-{warehouse_id}.html", "")
    parts = slug.split("-")

    if len(parts) >= 2 and len(parts[-1]) == 2:
        state = parts[-1].upper()
        city_slug = "-".join(parts[:-1])
    else:
        state = "US"
        city_slug = slug

    city = city_slug.replace("-", " ").title()
    return {"id": warehouse_id, "name": city, "state": state, "url": url}


def get_warehouses(base_dir: pathlib.Path = SCRIPT_DIR) -> list[dict[str, str]]:
    raw_urls = load_urls(base_dir)
    warehouses: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for url in raw_urls:
        info = parse_warehouse_info(url)
        if info and info["id"] not in seen_ids:
            warehouses.append(info)
            seen_ids.add(info["id"])

    return warehouses


def load_cookies(cookies_file: pathlib.Path) -> list[dict[str, Any]] | None:
    try:
        if cookies_file.exists():
            return json.loads(cookies_file.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.exception("Failed to read cookies from %s", cookies_file)
    return None


def save_cookies(cookies_file: pathlib.Path, cookies: list[dict[str, Any]]) -> None:
    try:
        cookies_file.parent.mkdir(parents=True, exist_ok=True)
        cookies_file.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
    except Exception:
        LOGGER.exception("Failed to write cookies to %s", cookies_file)


def cookie_header_from_list(cookies: list[dict[str, Any]]) -> str:
    return "; ".join(
        f"{cookie['name']}={cookie['value']}"
        for cookie in cookies
        if "name" in cookie and "value" in cookie
    )


async def refresh_cookies_interactive(config: ScraperConfig) -> list[dict[str, Any]]:
    if async_playwright is None:
        raise ScraperError("Playwright is not installed. Install with: pip install playwright")

    LOGGER.info("Opening browser to refresh cookies")
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=config.cookie_capture_headless)
        context = await browser.new_context(user_agent=config.headers["User-Agent"])
        page = await context.new_page()

        try:
            await page.goto("https://www.costco.com/", timeout=60000)
        except Exception as exc:
            LOGGER.warning("Failed to load Costco homepage during cookie refresh: %s", exc)

        start = time.time()
        cookies: list[dict[str, Any]] = []
        while time.time() - start < config.cookie_capture_wait_seconds:
            cookies = await context.cookies()
            names = {cookie.get("name") for cookie in cookies}
            if any(token in names for token in ("bm_s", "bm_sz", "_abck")):
                break
            await asyncio.sleep(1)

        cookies = await context.cookies()
        await browser.close()

    if not cookies:
        raise ScraperError("Cookie capture finished with no cookies")

    save_cookies(config.cookies_file, cookies)
    return cookies


def build_search_url(warehouse_id: str, state: str, config: ScraperConfig) -> str:
    base = "https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_navigation"

    loc_ids = [
        "580-bd",
        f"{warehouse_id}-wh",
        "1255-3pl",
        "1321-wm",
        "1468-3pl",
        "283-wm",
        "561-wm",
        "725-wm",
        "731-wm",
        "758-wm",
        "759-wm",
        "847_0-cor",
        "847_0-cwt",
        "847_0-edi",
        "847_0-ehs",
        "847_0-membership",
        "847_0-mpt",
        "847_0-spc",
        "847_0-wm",
        "847_1-cwt",
        "847_1-edi",
        "847_d-fis",
        "847_lg_n1a-edi",
        "847_lux_us41-edi",
        "847_NA-cor",
        "847_NA-pharmacy",
        "847_NA-wm",
        "847_ss_u358-edi",
        "847_wp_r452-edi",
        "951-wm",
        "952-wm",
        "9847-wcs",
    ]

    params = {
        "expoption": "lw",
        "q": "*:*",
        "locale": "en-US",
        "start": "0",
        "expand": "false",
        "userLocation": state,
        "loc": ",".join(loc_ids),
        "whloc": f"{warehouse_id}-wh",
        "rows": str(config.page_rows),
        "chdcategory": "true",
        "chdheader": "true",
    }

    return base + "?" + urlencode(params, safe=":(),")


def _backoff_sleep(config: ScraperConfig, attempt: int) -> None:
    time.sleep(config.retry_backoff_base * (2 ** max(0, attempt - 1)))


def _request_json_get_with_retry(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    config: ScraperConfig,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, config.max_retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=config.timeout)
            if response.status_code == 200:
                return response.json()

            message = f"HTTP {response.status_code} while fetching {url}"
            if response.status_code not in RETRIABLE_HTTP_STATUS or attempt == config.max_retries:
                raise ScraperError(message)
            LOGGER.warning("%s (attempt %s/%s)", message, attempt, config.max_retries)
        except (requests.RequestException, ValueError, ScraperError) as exc:
            last_error = exc
            if isinstance(exc, ScraperError) and attempt == config.max_retries:
                break
            if not isinstance(exc, ScraperError):
                LOGGER.warning("Request error on attempt %s/%s: %s", attempt, config.max_retries, exc)
            if attempt == config.max_retries:
                break
            _backoff_sleep(config, attempt)

    raise ScraperError(f"Failed to fetch search API data: {last_error}") from last_error


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((requests.RequestException, ValueError, ScraperError)),
)
def fetch_products_graphql(
    item_numbers: list[str],
    warehouse_number: str,
    config: ScraperConfig,
    cookie_string: str,
) -> dict[str, dict[str, Any]]:
    headers = config.headers.copy()
    headers["Cookie"] = cookie_string
    if config.x_api_key:
        headers["x-api-key"] = config.x_api_key

    variables = {
        "itemNumbers": item_numbers,
        "clientId": config.headers.get("client-identifier"),
        "locale": ["en-us"],
        "warehouseNumber": str(warehouse_number),
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

    response = requests.post(
        config.graphql_url,
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=max(60, config.timeout),
    )

    if response.status_code != 200:
        raise ScraperError(f"GraphQL request failed with HTTP {response.status_code}")

    payload = response.json()
    products = payload.get("data", {}).get("products") or {}
    output: dict[str, dict[str, Any]] = {}

    for catalog_data in listify(products.get("catalogData") or []):
        item_number = str(catalog_data.get("itemNumber") or "")
        if item_number:
            output[item_number] = products

    return output


def determine_order_channel(payload: dict[str, Any], requested_warehouse: str | None = None) -> str:
    del requested_warehouse
    warehouse_attr = False
    online_attr = False

    for cat in listify(payload.get("catalogData") or []):
        for attribute in listify(cat.get("attributes") or []):
            vals = [norm(attribute.get("key")), norm(attribute.get("value"))]
            if "online only" in vals:
                online_attr = True
            if "warehouse only" in vals:
                warehouse_attr = True

    child = payload.get("childData") or {}
    for cat in listify(child.get("catalogData") or []):
        for attribute in listify(cat.get("attributes") or []):
            vals = [norm(attribute.get("key")), norm(attribute.get("value"))]
            if "online only" in vals:
                online_attr = True
            if "warehouse only" in vals:
                warehouse_attr = True

    warehouse_programs = {"inwarehouse", "warehouse", "locationcontrolledinventory", "warehousedelivery"}
    online_programs = {
        "2daydelivery",
        "ecommerce",
        "shipit",
        "3rdpartydelivery",
        "standard",
        "businessdelivery",
        "costcogrocery",
        "coldandfrozen",
        "googlegrocery",
    }

    for cat in listify(payload.get("catalogData") or []) + listify(child.get("catalogData") or []):
        program_types = cat.get("programTypes")
        if isinstance(program_types, str):
            tokens = set(token.strip().lower() for token in program_types.split(","))
            if tokens & warehouse_programs:
                warehouse_attr = True
            if tokens & online_programs:
                online_attr = True

    if warehouse_attr and online_attr:
        return "any"
    if warehouse_attr:
        return "warehouse_only"
    if online_attr:
        return "online_only"
    return "any"


def normalize_doc(
    search_doc: dict[str, Any],
    product_graph_map: dict[str, dict[str, Any]],
    warehouse_name: str,
    warehouse_id: str,
) -> dict[str, Any]:
    item_number = (
        search_doc.get("item_number")
        or search_doc.get("item_location_itemNumber")
        or search_doc.get("itemNumber")
        or ""
    )

    row = {
        "warehouse_id": warehouse_id,
        "warehouse_name": warehouse_name,
        "item_number": item_number,
        "name": search_doc.get("item_product_name") or search_doc.get("name") or "",
        "price": search_doc.get("item_location_pricing_salePrice", search_doc.get("minSalePrice", "")),
        "product_pic": search_doc.get("item_collateral_primaryimage") or search_doc.get("image") or "",
        "availability": search_doc.get("item_location_availability", ""),
    }

    payload = product_graph_map.get(item_number)

    search_doc_online = False
    search_doc_warehouse = False
    badges = (
        listify(search_doc.get("item_pill_attributes") or [])
        + listify(search_doc.get("Warehouse_Only_attr_pill") or [])
        + listify(search_doc.get("Online_Only_attr_pill") or [])
    )

    for badge in badges:
        normalized = norm(badge)
        if "online only" in normalized:
            search_doc_online = True
        if "warehouse only" in normalized:
            search_doc_warehouse = True

    if payload:
        if not row["price"]:
            for product_data in listify(payload.get("catalogData")):
                if product_data.get("priceData"):
                    row["price"] = product_data["priceData"].get("price")
                    break

        row["order_channel"] = determine_order_channel(payload, warehouse_id)
        if row["order_channel"] == "any":
            if search_doc_warehouse:
                row["order_channel"] = "warehouse_only"
            elif search_doc_online:
                row["order_channel"] = "online_only"
    else:
        if search_doc_warehouse:
            row["order_channel"] = "warehouse_only"
        elif search_doc_online:
            row["order_channel"] = "online_only"
        else:
            row["order_channel"] = "any"

    return row


def paginate_api(
    session: requests.Session,
    search_url: str,
    headers: dict[str, str],
    config: ScraperConfig,
) -> list[dict[str, Any]]:
    parsed = urlparse(search_url)
    query_string = dict(parse_qsl(parsed.query, keep_blank_values=True))
    start = int(query_string.get("start", "0"))

    all_docs: list[dict[str, Any]] = []
    num_found: int | None = None

    while True:
        query_string["start"] = str(start)
        page_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urlencode(query_string, doseq=True, safe=":(),"),
                parsed.fragment,
            )
        )

        payload = _request_json_get_with_retry(session, page_url, headers, config)
        response_data = payload.get("response", {})
        docs = response_data.get("docs", [])

        if num_found is None:
            num_found = response_data.get("numFound")
            LOGGER.info("Total items found: %s", num_found)

        if not docs:
            break

        all_docs.extend(docs)
        print(f"Fetched {len(all_docs)}/{num_found} items...", end="\r")

        rows = int(query_string.get("rows", config.page_rows))
        if num_found and len(all_docs) >= int(num_found):
            break
        if len(docs) < rows:
            break

        start += rows
        time.sleep(config.min_request_interval)

    print()
    return all_docs


def enrich_and_save(
    docs: list[dict[str, Any]],
    warehouse_info: dict[str, str],
    config: ScraperConfig,
    cookie_string: str,
) -> dict[str, Any]:
    warehouse_id = warehouse_info["id"]
    warehouse_name = warehouse_info["name"]

    item_numbers = []
    for doc in docs:
        number = doc.get("item_number") or doc.get("item_location_itemNumber") or doc.get("itemNumber")
        if number:
            item_numbers.append(str(number))

    unique_items = sorted(set(item_numbers))
    LOGGER.info("Enriching %s unique items via GraphQL", len(unique_items))

    product_graph_map: dict[str, dict[str, Any]] = {}
    failed_batches = 0

    for index in range(0, len(unique_items), config.graphql_batch_size):
        batch = unique_items[index : index + config.graphql_batch_size]
        try:
            mapping = fetch_products_graphql(batch, warehouse_id, config, cookie_string)
            product_graph_map.update(mapping)
            print(f"Enriched {len(product_graph_map)} items...", end="\r")
        except Exception as exc:
            failed_batches += 1
            LOGGER.warning("GraphQL batch failed for warehouse %s: %s", warehouse_id, exc)

        time.sleep(config.min_request_interval)

    print()

    rows = [normalize_doc(doc, product_graph_map, warehouse_name, warehouse_id) for doc in docs]
    safe_name = "".join(character if character.isalnum() else "_" for character in warehouse_name)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.output_dir / f"costco_scrape_{warehouse_id}_{safe_name}_products.csv"

    data_frame = pd.DataFrame(rows)
    data_frame.to_csv(output_path, index=False)

    LOGGER.info("Saved %s rows to %s", len(data_frame), output_path)
    return {
        "rows": len(data_frame),
        "output_file": str(output_path),
        "enriched_items": len(product_graph_map),
        "failed_graphql_batches": failed_batches,
    }


def scrape_warehouse(
    target: dict[str, str],
    *,
    config: ScraperConfig | None = None,
    session: requests.Session | None = None,
    allow_cookie_refresh: bool = True,
) -> dict[str, Any]:
    config = config or ScraperConfig.from_env()
    session = session or requests.Session()

    summary: dict[str, Any] = {
        "warehouse_id": target.get("id"),
        "warehouse_name": target.get("name"),
        "success": False,
        "rows": 0,
        "output_file": None,
        "error": None,
    }

    print(f"Starting scrape for: {target['name']} (ID: {target['id']})")

    cookies = load_cookies(config.cookies_file)
    if not cookies:
        if not allow_cookie_refresh:
            summary["error"] = "No cookies found and cookie refresh is disabled"
            return summary

        print("No cookies found. Launching browser to capture cookies (Playwright)...")
        try:
            cookies = asyncio.run(refresh_cookies_interactive(config))
        except Exception as exc:
            summary["error"] = f"Failed to capture cookies: {exc}"
            return summary

    cookie_string = cookie_header_from_list(cookies)
    headers = {**config.headers, "Cookie": cookie_string}
    if config.x_api_key:
        headers["x-api-key"] = config.x_api_key

    search_url = build_search_url(target["id"], target["state"], config)

    try:
        docs = paginate_api(session, search_url, headers, config)
    except Exception as exc:
        summary["error"] = f"Search API failed: {exc}"
        return summary

    if not docs and allow_cookie_refresh:
        print("No items found. Attempting automatic cookie refresh...")
        try:
            cookies = asyncio.run(refresh_cookies_interactive(config))
            cookie_string = cookie_header_from_list(cookies)
            headers["Cookie"] = cookie_string
            docs = paginate_api(session, search_url, headers, config)
        except Exception as exc:
            summary["error"] = f"Cookie refresh retry failed: {exc}"
            return summary

    if not docs:
        summary["error"] = "No items returned for this warehouse"
        return summary

    try:
        enrichment = enrich_and_save(docs, target, config, cookie_string)
    except Exception as exc:
        summary["error"] = f"Failed to enrich/save results: {exc}"
        return summary

    summary.update(enrichment)
    summary["success"] = True
    print("Scrape completed successfully.")
    return summary


def find_warehouse_matches(warehouses: list[dict[str, str]], query: str) -> list[dict[str, str]]:
    needle = query.strip().lower()
    if not needle:
        return []
    return [warehouse for warehouse in warehouses if needle in warehouse["name"].lower() or needle == warehouse["id"]]


def choose_warehouse_interactive(matches: list[dict[str, str]], limit: int = 20) -> dict[str, str] | None:
    print("\nMatches:")
    for index, warehouse in enumerate(matches[:limit]):
        print(f"{index}: {warehouse['name']} ({warehouse['state']}) - ID: {warehouse['id']}")

    try:
        idx = int(input("\nEnter selection number: ").strip())
        return matches[idx]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape Costco warehouse inventory and pricing.")
    parser.add_argument("--search", help="Warehouse name or ID search query.")
    parser.add_argument("--warehouse-id", help="Direct warehouse ID selection.")
    parser.add_argument(
        "--list-limit",
        type=int,
        default=20,
        help="Maximum number of matches to show in interactive selection.",
    )
    parser.add_argument("--output-dir", help="Directory to save output CSV files.")
    parser.add_argument("--no-cookie-refresh", action="store_true", help="Disable opening browser to refresh cookies.")
    parser.add_argument("--log-level", default=None, help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = ScraperConfig.from_env(output_dir=args.output_dir, log_level=args.log_level)

    try:
        warehouses = get_warehouses()
    except ScraperError as exc:
        print(f"Failed to load warehouse URLs: {exc}")
        return 1

    print(f"Loaded {len(warehouses)} warehouses.")

    target: dict[str, str] | None = None

    if args.warehouse_id:
        target = next((w for w in warehouses if w["id"] == args.warehouse_id.strip()), None)
        if not target:
            print(f"Warehouse ID {args.warehouse_id} not found.")
            return 1
    else:
        query = args.search or input("Enter warehouse name or ID to search: ").strip()
        matches = find_warehouse_matches(warehouses, query)
        if not matches:
            print("No matches found.")
            return 1
        target = choose_warehouse_interactive(matches, limit=max(1, args.list_limit))
        if not target:
            return 1

    result = scrape_warehouse(
        target,
        config=config,
        allow_cookie_refresh=not args.no_cookie_refresh,
    )

    if result["success"]:
        print(f"Completed: {result['rows']} rows -> {result['output_file']}")
        if result.get("failed_graphql_batches"):
            print(f"Warning: {result['failed_graphql_batches']} GraphQL batches failed during enrichment.")
        return 0

    print(f"Scrape failed: {result['error']}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
