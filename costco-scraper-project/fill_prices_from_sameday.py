#!/usr/bin/env python3
# fill_sameday_prices_items_with_fallback.py
# pip install requests pandas tenacity

import json, time, pathlib, re, uuid
from urllib.parse import urlencode
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# === CONFIG - INSERT YOUR VALUES FROM DEVTOOLS ===
INPUT = pathlib.Path("grocery_all_products.csv")
OUT = pathlib.Path("grocery_all_products_with_sameday_prices_items_fallback.csv")
REPORT = pathlib.Path("sameday_price_report_items_fallback.csv")
CHUNKSIZE = 50000
DELAY = 0.6
TIMEOUT = 30

GRAPHQL_URL = "https://sameday.costco.com/graphql"

# persisted hashes from your captures
ITEMS_PERSISTED_HASH = "8e814b738f12564460af5db399c598e09807f9c34cd3f959763f81be058d9c24"
DISCOVERY_PERSISTED_HASH = "0d8601d6f4adc2c7f37b5256e4a135d0f5de96d4740800693ded80182eca61e9"
SEARCH_PERSISTED_HASH = "064939d1cbbdcc31c0dfd663955db46ef5f0fa3919f203adc8ecac058c050380"

# REQUIRED: paste complete Cookie header and x-ic-qp/x-page-view-id from a working browser request
COOKIE_STRING = ""         # e.g., "AMCV_...=...; _abck=...; ..."
X_IC_QP = ""               # e.g., "f4f5396d-..."
X_PAGE_VIEW_ID = ""        # e.g., "43fb1bd3-..."

# default site/store context (change if needed)
SHOP_ID = "6481"
ZONE_ID = "278"
POSTAL_CODE = "60525"
RETAILER_LOCATION_ID = "1091"

# === Headers/session ===
HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Referer": "https://sameday.costco.com/",
    "User-Agent": "Mozilla/5.0",
    "x-client-identifier": "web",
    "x-client-user-id": "18522408761470180",
    "x-ic-view-layer": "true",
}
if COOKIE_STRING:
    HEADERS["Cookie"] = COOKIE_STRING
if X_IC_QP:
    HEADERS["x-ic-qp"] = X_IC_QP
if X_PAGE_VIEW_ID:
    HEADERS["x-page-view-id"] = X_PAGE_VIEW_ID

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.max_redirects = 5

PRICE_RE = re.compile(r"\$\s*[\d,]+(?:\.\d{2})?")

# === Network helpers with retries ===
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def items_persisted_lookup(item_tokens):
    vars = {"ids": item_tokens, "shopId": SHOP_ID, "zoneId": ZONE_ID, "postalCode": POSTAL_CODE}
    params = {
        "operationName": "Items",
        "variables": json.dumps(vars, separators=(',', ':')),
        "extensions": json.dumps({"persistedQuery": {"version": 1, "sha256Hash": ITEMS_PERSISTED_HASH}}, separators=(',', ':'))
    }
    r = SESSION.get(GRAPHQL_URL, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def discovery_lookup(anchor_ids):
    # anchor_ids: list of product ids (strings) to seed discovery; server may return related items with prices
    vars = {"anchorProductIds": anchor_ids, "discoverType": "replacementRecommendations", "first": 6,
            "pageViewId": str(uuid.uuid4()), "shopId": SHOP_ID, "retailerInventorySessionToken": None,
            "postalCode": POSTAL_CODE, "zoneId": ZONE_ID}
    payload = {"operationName":"DiscoveryDiscoverItems","variables":vars,"extensions":{"persistedQuery":{"version":1,"sha256Hash":DISCOVERY_PERSISTED_HASH}}}
    r = SESSION.post(GRAPHQL_URL, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def search_lookup(query_text):
    vars = {"filters": [], "action": None, "query": query_text, "pageViewId": str(uuid.uuid4()),
            "retailerInventorySessionToken": None, "searchSource": "search", "orderBy": "bestMatch",
            "contentManagementSearchParams": {"itemGridColumnCount": 7},
            "shopId": SHOP_ID, "postalCode": POSTAL_CODE, "zoneId": ZONE_ID, "first": 6}
    payload = {"operationName":"SearchResultsPlacements","variables":vars,"extensions":{"persistedQuery":{"version":1,"sha256Hash":SEARCH_PERSISTED_HASH}}}
    r = SESSION.post(GRAPHQL_URL, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# === JSON price extractors ===
def normalize_price_str(p):
    if p is None:
        return None
    s = str(p).strip()
    m = PRICE_RE.search(s)
    if m:
        raw = m.group(0)
        val = raw.replace("$", "").replace(",", "").strip()
        try:
            return float(val)
        except:
            return None
    try:
        return float(s)
    except:
        return None

def extract_prices_from_items_response(j):
    """
    Returns dict token->float_or_None for each item in the response.
    Tokens expected: id field like "items_1091-17681167"
    """
    out = {}
    if not isinstance(j, dict):
        return out
    items = j.get("data", {}).get("items") or j.get("data", {}).get("itemsById")
    if isinstance(items, list):
        items_list = items
    elif isinstance(items, dict):
        items_list = list(items.values())
    else:
        items_list = []
    for it in items_list:
        token = it.get("id") or it.get("itemId") or None
        if not token:
            continue
        out[token] = None
        price_obj = it.get("price") or {}
        # direct fields
        for k in ("priceString", "priceValueString", "priceValue", "price"):
            v = price_obj.get(k) if isinstance(price_obj, dict) else None
            if v:
                val = normalize_price_str(v)
                if val is not None:
                    out[token] = val
                    break
        if out[token] is not None:
            continue
        # nested fallbacks
        vs = price_obj.get("viewSection", {}) if isinstance(price_obj, dict) else {}
        for sub in ("itemDetails", "itemCard"):
            cand = vs.get(sub, {}) if isinstance(vs, dict) else {}
            if isinstance(cand, dict):
                for k in ("priceString", "priceAriaLabelString", "priceValueString"):
                    if k in cand and cand[k]:
                        val = normalize_price_str(cand[k])
                        if val is not None:
                            out[token] = val
                            break
            if out[token] is not None:
                break
    return out

def extract_price_from_discovery(j, wanted_product_ids):
    """
    Parse discovery response and map productId -> price if present in discoverItems.items.
    Returns dict of productId->price
    """
    mapping = {}
    try:
        items = j.get("data", {}).get("discoverItems", {}).get("items", [])
    except Exception:
        items = []
    for it in items:
        pid = it.get("productId") or it.get("product_id") or None
        if not pid:
            continue
        price_val = None
        price_obj = it.get("price") or {}
        # reuse normalize logic
        for k in ("priceString","priceValueString","priceValue"):
            if isinstance(price_obj, dict) and k in price_obj and price_obj[k]:
                price_val = normalize_price_str(price_obj[k])
                if price_val is not None:
                    break
        if price_val is None:
            vs = price_obj.get("viewSection", {}) if isinstance(price_obj, dict) else {}
            for sub in ("itemDetails","itemCard"):
                cand = vs.get(sub, {}) if isinstance(vs, dict) else {}
                if isinstance(cand, dict):
                    for kk in ("priceString","priceAriaLabelString"):
                        if kk in cand and cand[kk]:
                            price_val = normalize_price_str(cand[kk])
                            if price_val is not None:
                                break
                if price_val is not None:
                    break
        if price_val is not None:
            mapping[str(pid)] = price_val
    return mapping

def extract_price_from_search(j):
    # similar walker for SearchResultsPlacements responses; find first price under data.items
    # returns dict productId->price
    mapping = {}
    try:
        items = j.get("data", {}).get("items", []) or j.get("data", {}).get("searchResults", {}).get("items", [])
    except Exception:
        items = []
    for it in items:
        pid = it.get("productId") or it.get("product_id") or None
        if not pid:
            continue
        price_val = None
        price_obj = it.get("price") or {}
        if isinstance(price_obj, dict):
            for k in ("priceString","priceValueString","priceValue"):
                if k in price_obj and price_obj[k]:
                    price_val = normalize_price_str(price_obj[k])
                    if price_val is not None:
                        break
            if price_val is None:
                vs = price_obj.get("viewSection", {}) if isinstance(price_obj, dict) else {}
                for sub in ("itemDetails","itemCard"):
                    cand = vs.get(sub, {}) if isinstance(vs, dict) else {}
                    if isinstance(cand, dict):
                        for kk in ("priceString","priceAriaLabelString"):
                            if kk in cand and cand[kk]:
                                price_val = normalize_price_str(cand[kk])
                                if price_val is not None:
                                    break
                    if price_val is not None:
                        break
        if price_val is not None:
            mapping[str(pid)] = price_val
    return mapping

# === CSV helpers ===
def detect_columns(df):
    id_candidates = [c for c in df.columns if c.lower() in ("item_number","sku","product_id","id","itemid","productid")]
    name_candidates = [c for c in df.columns if c.lower() in ("name","product_name","title")]
    price_candidates = [c for c in df.columns if c.lower() in ("price","saleprice","sale_price","item_price","current_price")]
    loc_candidates = [c for c in df.columns if "retailer_location" in c.lower() or "retailer_location_id" in c.lower() or "warehouse" in c.lower()]
    return (id_candidates[0] if id_candidates else None,
            name_candidates[0] if name_candidates else None,
            price_candidates[0] if price_candidates else None,
            loc_candidates[0] if loc_candidates else None)

# === Main streaming process with fallbacks ===
def stream_process():
    if not INPUT.exists():
        raise SystemExit(f"Missing input file: {INPUT}")
    df0 = pd.read_csv(INPUT, nrows=0)
    all_cols = df0.columns.tolist()
    sample = pd.read_csv(INPUT, nrows=1000)
    id_col, name_col, price_col, loc_col = detect_columns(sample)
    if not price_col:
        price_col = "price"
    print("Detected columns -> id:", id_col, "name:", name_col, "price:", price_col, "loc:", loc_col)

    out_cols = all_cols.copy()
    if price_col not in out_cols:
        out_cols.append(price_col)
    if "price_source" not in out_cols:
        out_cols.append("price_source")
    if "price_source_url" not in out_cols:
        out_cols.append("price_source_url")

    OUT.unlink(missing_ok=True)
    pd.DataFrame(columns=out_cols).to_csv(OUT, index=False)
    REPORT.unlink(missing_ok=True)
    pd.DataFrame(columns=["row_offset","id","name","found","price","source","url","error"]).to_csv(REPORT, index=False)

    cache = {}
    row_offset = 0

    for chunk in pd.read_csv(INPUT, chunksize=CHUNKSIZE, dtype=str, low_memory=False):
        chunk = chunk.fillna("")
        if price_col not in chunk.columns:
            chunk[price_col] = ""
        report_rows = []
        missing_idx = chunk[chunk[price_col].astype(str).str.strip() == ""].index.tolist()

        # Build tokens for rows with numeric product ids
        tokens_to_lookup = []
        token_row_map = {}
        name_fallback_rows = []
        for idx in missing_idx:
            row = chunk.loc[idx]
            pid = ""
            if id_col and id_col in row and str(row[id_col]).strip():
                pid = str(row[id_col]).strip()
            if pid and pid.isdigit():
                retailer_loc = (str(row[loc_col]).strip() if loc_col and loc_col in row and str(row[loc_col]).strip() else RETAILER_LOCATION_ID)
                token = f"items_{retailer_loc}-{pid}"
                token_row_map.setdefault(token, []).append(idx)
                if token not in cache and token not in tokens_to_lookup:
                    tokens_to_lookup.append(token)
            else:
                name_fallback_rows.append(idx)

        # Step 1: Items persisted lookup in batches
        BATCH = 20
        for i in range(0, len(tokens_to_lookup), BATCH):
            batch = tokens_to_lookup[i:i+BATCH]
            try:
                resp = items_persisted_lookup(batch)
                res_map = extract_prices_from_items_response(resp)
                src_url = GRAPHQL_URL + "?" + urlencode({"operationName":"Items"})
                for token in batch:
                    cache[token] = (res_map.get(token), src_url)
            except Exception as e:
                for token in batch:
                    cache[token] = (None, "")
            time.sleep(DELAY)

        # Step 2: For tokens with price None, call Discovery using productIds (seed), then re-request Items
        tokens_needing_seed = [t for t, (v,u) in cache.items() if v is None and t in token_row_map]
        if tokens_needing_seed:
            # extract product ids to pass to discovery (from token pattern)
            product_ids = []
            for t in tokens_needing_seed:
                parts = t.split("-", 1)
                if len(parts) == 2 and parts[1].isdigit():
                    product_ids.append(parts[1])
            if product_ids:
                try:
                    disc_resp = discovery_lookup(product_ids)
                    disc_map = extract_prices_from_items_response(disc_resp)  # sometimes discover returns items under data.discoverItems.items -> handled below
                    # also parse discoverItems prices by product id
                    prod_map = extract_price_from_discovery(disc_resp, product_ids)
                except Exception:
                    disc_resp = None
                    prod_map = {}
                # re-request Items for the same tokens to pull now-populated price fields
                for i in range(0, len(tokens_needing_seed), BATCH):
                    batch = tokens_needing_seed[i:i+BATCH]
                    try:
                        r2 = items_persisted_lookup(batch)
                        rmap = extract_prices_from_items_response(r2)
                        for token in batch:
                            if rmap.get(token) is not None:
                                cache[token] = (rmap.get(token), GRAPHQL_URL + "?" + urlencode({"operationName":"Items"}))
                            else:
                                # if discovery returned price by product id, use that
                                pid = token.split("-",1)[1]
                                if pid in prod_map:
                                    cache[token] = (prod_map[pid], GRAPHQL_URL + "?" + urlencode({"operationName":"DiscoveryDiscoverItems"}))
                                else:
                                    cache[token] = (None, GRAPHQL_URL + "?" + urlencode({"operationName":"Items"}))
                    except Exception:
                        for token in batch:
                            cache[token] = (None, "")
                    time.sleep(DELAY)

        # Step 3: name-based fallback for rows without numeric PID or still missing prices: run search then re-request Items for any discovered product ids
        # For simplicity we process name_fallback_rows one-by-one (could be batched)
        for idx in name_fallback_rows:
            row = chunk.loc[idx]
            q = (row[name_col] if name_col and name_col in row else "").strip()
            if not q:
                report_rows.append({"row_offset": row_offset+idx, "id": row.get(id_col,""), "name": row.get(name_col,""), "found": False, "price": "", "source": "", "url": "", "error": "no name/id"})
                continue
            try:
                sresp = search_lookup(q)
                search_prices = extract_price_from_search(sresp)
                # if search returned productId->price, map them to tokens and write
                if search_prices:
                    # try to map first matching productId to this row
                    for pid, priceval in search_prices.items():
                        retail_loc = RETAILER_LOCATION_ID
                        tkn = f"items_{retail_loc}-{pid}"
                        cache[tkn] = (priceval, GRAPHQL_URL + "?" + urlencode({"operationName":"SearchResultsPlacements"}))
                        # assign to this row and break
                        chunk.at[idx, price_col] = priceval
                        chunk.at[idx, "price_source"] = "sameday_search_graphql"
                        chunk.at[idx, "price_source_url"] = GRAPHQL_URL + "?" + urlencode({"operationName":"SearchResultsPlacements"})
                        report_rows.append({"row_offset": row_offset+idx, "id": row.get(id_col,""), "name": row.get(name_col,""), "found": True, "price": priceval, "source": "sameday_search_graphql", "url": GRAPHQL_URL + "?" + urlencode({"operationName":"SearchResultsPlacements"}), "error": ""})
                        break
                    if search_prices:
                        continue
                # if no price found via search
                report_rows.append({"row_offset": row_offset+idx, "id": row.get(id_col,""), "name": row.get(name_col,""), "found": False, "price": "", "source": "sameday_search_graphql", "url": GRAPHQL_URL + "?" + urlencode({"operationName":"SearchResultsPlacements"}), "error": "search not found"})
            except Exception as e:
                report_rows.append({"row_offset": row_offset+idx, "id": row.get(id_col,""), "name": row.get(name_col,""), "found": False, "price": "", "source": "", "url": "", "error": str(e)})
            time.sleep(DELAY)

        # Step 4: write results for token-based rows using cache
        for token, indices in list(token_row_map.items()):
            price_val, src_url = cache.get(token, (None, ""))
            for idx in indices:
                row = chunk.loc[idx]
                if price_val is not None:
                    chunk.at[idx, price_col] = price_val
                    chunk.at[idx, "price_source"] = "sameday_items_graphql"
                    chunk.at[idx, "price_source_url"] = src_url
                    report_rows.append({"row_offset": row_offset+idx, "id": row.get(id_col,""), "name": row.get(name_col,""), "found": True, "price": price_val, "source": "sameday_items_graphql", "url": src_url, "error": ""})
                else:
                    chunk.at[idx, "price_source"] = "sameday_items_graphql"
                    chunk.at[idx, "price_source_url"] = src_url
                    report_rows.append({"row_offset": row_offset+idx, "id": row.get(id_col,""), "name": row.get(name_col,""), "found": False, "price": "", "source": "sameday_items_graphql", "url": src_url, "error": "not found"})

        # append chunk and report rows
        chunk.to_csv(OUT, index=False, header=False, mode="a")
        if report_rows:
            pd.DataFrame(report_rows).to_csv(REPORT, index=False, header=False, mode="a")
        row_offset += len(chunk)

    print("Done. Outputs:", OUT, REPORT)

if __name__ == "__main__":
    stream_process()
