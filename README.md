# Costco Products Scraper

A Costco warehouse product scraper with both CLI and GUI interfaces, including pagination handling, GraphQL enrichment, and CSV exports.

## Done Criteria

The project is considered fully finished for `v1.x` when all of the following remain true:

- **Supported OSes**: Linux, macOS, and Windows are passing in CI.
- **Scrape reliability target**: >= 95% successful runs for valid warehouses under normal network conditions.
- **Runtime target**: Typical single-warehouse run completes within 3-10 minutes depending on item count.
- **Output schema stability**: CSV columns remain stable unless a breaking version is released.
- **Maintenance scope**: Public package-quality repository with tests, CI, security checks, and release workflow.

## Features

- CLI and GUI interfaces.
- Search API pagination support.
- GraphQL enrichment for pricing and channel hints.
- Environment-driven runtime configuration.
- Batch warehouse scraping from GUI.

## Quick Start

### 1) Install

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) (Optional) Install Playwright browser

```bash
playwright install chromium
```

### 3) Run CLI

```bash
costco-scraper --search "Chicago"
```

Or by module path:

```bash
python /home/runner/work/Costco-products-scraper/Costco-products-scraper/costco-scraper-project/costco_scraper.py --search "Chicago"
```

### 4) Run GUI

```bash
costco-scraper-gui
```

Or:

```bash
python /home/runner/work/Costco-products-scraper/Costco-products-scraper/costco-scraper-project/costco_gui.py
```

## CLI Usage

```bash
costco-scraper --help
```

Common options:
- `--search <text>`: Search warehouse by name or ID.
- `--warehouse-id <id>`: Direct selection by ID.
- `--output-dir <path>`: Output CSV directory.
- `--no-cookie-refresh`: Disable interactive cookie refresh.
- `--log-level <level>`: `DEBUG|INFO|WARNING|ERROR`.

## Configuration

Runtime behavior is controlled by environment variables:

- `COSTCO_OUTPUT_DIR`
- `COSTCO_COOKIES_FILE`
- `COSTCO_PAGE_ROWS`
- `COSTCO_TIMEOUT`
- `COSTCO_MAX_RETRIES`
- `COSTCO_RETRY_BACKOFF_BASE`
- `COSTCO_MIN_REQUEST_INTERVAL`
- `COSTCO_GRAPHQL_BATCH_SIZE`
- `COSTCO_COOKIE_CAPTURE_WAIT_SECONDS`
- `COSTCO_COOKIE_CAPTURE_HEADLESS`
- `COSTCO_X_API_KEY`
- `COSTCO_CLIENT_IDENTIFIER`
- `COSTCO_LOG_LEVEL`

## Output Schema

CSV files are written as:

`costco_scrape_<warehouse_id>_<warehouse_name>_products.csv`

Columns:
- `warehouse_id`
- `warehouse_name`
- `item_number`
- `name`
- `price`
- `product_pic`
- `availability`
- `order_channel` (`warehouse_only`, `online_only`, `any`)

## Architecture

- `/costco-scraper-project/costco_scraper.py`: core scraping, retry/error handling, CLI.
- `/costco-scraper-project/costco_gui.py`: GUI batch interface.
- `/costco-scraper-project/urls_part*.json`: source warehouse location URLs.
- `/tests`: unit, integration-style, and regression tests.

## Quality Gates

CI runs on push and pull request:
- `ruff check .`
- `pytest`

Security workflow includes:
- dependency audit (`pip-audit`)
- secret scanning (`gitleaks`)

## Common Failures & Fixes

- **No cookies found**: run without `--no-cookie-refresh` and complete browser challenge.
- **0 items returned**: retry; cookie may be stale or API may be throttling.
- **GraphQL batch warnings**: output CSV is still created; rerun for full enrichment.
- **Playwright import error**: `pip install playwright`.

## Safe Usage Guidance

- Keep moderate request rates (avoid reducing `COSTCO_MIN_REQUEST_INTERVAL` aggressively).
- Use retry defaults unless diagnosing.
- Avoid parallel runs against many warehouses from multiple sessions simultaneously.

## Release Workflow

- Version is tracked in `/VERSION` and changelog in `/CHANGELOG.md`.
- Tag releases as `v*` (for example `v1.0.1`) to trigger release notes workflow.
- Maintenance process is documented in `/CONTRIBUTING.md` and `/ROADMAP.md`.
