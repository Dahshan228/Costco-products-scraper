# Costco Warehouse Web Scraper

A modular and robust web scraper for Costco warehouses, capable of fetching product data, prices, and inventory status. It pages through Costco's catalog search API from inside a real browser and offers both a Command Line Interface (CLI) and a Graphical User Interface (GUI).

## Features

-   **Dual Interface**:
    -   **CLI (`costco_scraper.py`)**: Interactive command-line tool for quick, single-warehouse scraping.
    -   **GUI (`costco_gui.py`)**: Modern GUI built with `ttkbootstrap` supporting search, filtering, and batch scraping of multiple warehouses.
-   **In-browser scraping**:
    -   Costco's search API (`gdx-api.costco.com`) is protected by Kasada, which attaches a fresh token to every request made by Costco's own page script. The scraper opens a visible Chromium window with **Playwright** and sends its requests from inside the page, so the tokens are handled for it. Headless Chromium is blocked, so the window has to stay open while it runs.
    -   One empty query returns a warehouse's whole catalog (about 12,500 products), fetched 100 at a time.
-   **Warehouse vs online prices**:
    -   Costco reports the in-warehouse price and the delivery price separately, and they differ (e.g. $26.49 in the warehouse vs $34.49 online). Both are saved, and the warehouse price is never filled from the online one.
    -   Classifies items as "warehouse_only", "online_only", or "any".
-   **Data Output**:
    -   Saves results to structured CSV files (`costco_scrape_[ID]_[Name]_products.csv`).

## Prerequisites

-   Python 3.8+
-   Google Chrome or Chromium (managed by Playwright)

## Installation

1.  **Clone the repository** (if you haven't already):
    ```bash
    git clone https://github.com/Dahshan228/Costco-products-scraper.git
    cd Costco-products-scraper
    ```

2.  **Install Python Dependencies**:
    ```bash
    pip install playwright ttkbootstrap
    ```

3.  **Install Playwright Browsers**:
    The scraper drives this browser to make its requests.
    ```bash
    playwright install chromium
    ```

## Usage

### 1. Graphical User Interface (GUI)

The GUI is the recommended way to use the scraper, especially for scraping multiple locations.

1.  Run the GUI application:
    ```bash
    python costco-scraper-project/costco_gui.py
    ```
2.  **Search/Filter**: Type in the search box to find specific warehouses by city, state, or ID.
3.  **Select**: Click to select warehouses. You can select multiple warehouses (hold Ctrl/Cmd or Shift).
4.  **Scrape**: Click "Scrape Selected Warehouses".
5.  **Monitor**: Watch the "Live Log Output" for progress.
6.  **Results**: CSV files will be generated in the project directory.

### 2. Command Line Interface (CLI)

1.  Run the scraper script:
    ```bash
    python costco-scraper-project/costco_scraper.py
    ```
2.  The script will load available warehouses.
3.  Enter a search term (e.g., "Chicago" or "123").
4.  Select the desired warehouse from the numbered list.
5.  The scraper will run and save the data to a CSV file.

## Output Format

The generated CSV files contain the following columns:

-   `warehouse_id`: The unique ID of the Costco warehouse.
-   `warehouse_name`: Location name (e.g., "Oak Brook").
-   `item_number`: Costco item number (one row per item number; a product with several variants gives several rows).
-   `product_id`: Costco's product id, shared by the variants of one product.
-   `name`, `brand`, `category`: Product details.
-   `price`: **In-warehouse** price at this warehouse. Empty when the item is not sold or is out of stock there. For products with several variants this is the lowest variant price.
-   `online_price`: Price for delivery/online order.
-   `availability`: Stock at this warehouse: "in stock", "low stock", "out of stock", or empty when the warehouse does not carry it.
-   `order_channel`: "warehouse_only", "online_only", "any", or empty when Costco gives no price at all.
-   `product_pic`: URL to the product image.

## Project Structure

-   `costco_scraper.py`: Core scraping logic, API interaction, and CLI entry point.
-   `costco_gui.py`: Tkinter/ttkbootstrap GUI wrapper.
-   `urls_part1.json`, `urls_part2.json`: Database of Costco warehouse URLs.
