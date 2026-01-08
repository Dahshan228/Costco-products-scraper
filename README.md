# Costco Warehouse Web Scraper

A modular and robust web scraper for Costco warehouses, capable of fetching product data, prices, and inventory status. This tool integrates warehouse selection with powerful API scraping (Search + GraphQL) and offers both a Command Line Interface (CLI) and a Graphical User Interface (GUI).

## Features

-   **Dual Interface**:
    -   **CLI (`costco_scraper.py`)**: Interactive command-line tool for quick, single-warehouse scraping.
    -   **GUI (`costco_gui.py`)**: Modern GUI built with `ttkbootstrap` supporting search, filtering, and batch scraping of multiple warehouses.
-   **Robust Scraping**:
    -   Uses **Playwright** for handling cookies and bypassing bot protections.
    -   Uses **Requests** for efficient API data fetching.
    -   **GraphQL Integration**: Enriches search results with detailed product info (real-time pricing, inventory status, attributes).
-   **Smart Filtering**:
    -   Distinguishes between "Online Only", "Warehouse Only", and "ShipIt" items.
    -   automatically detects and handles pagination.
-   **Data Output**:
    -   Saves results to structured CSV files (`costco_scrape_[ID]_[Name]_products.csv`).

## Prerequisites

-   Python 3.8+
-   Google Chrome or Chromium (managed by Playwright)

## Installation

1.  **Clone the repository** (if you haven't already):
    ```bash
    git clone https://github.com/Dahshan228/Costco-web-scraper.git
    cd Costco-web-scraper
    ```

2.  **Install Python Dependencies**:
    ```bash
    pip install pandas tenacity playwright requests ttkbootstrap
    ```

3.  **Install Playwright Browsers**:
    This is required for the scraper to handle cookies and authentication tokens.
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
-   `item_number`: Product SKU.
-   `name`: Product name.
-   `price`: Current sale price (enriched via GraphQL).
-   `product_pic`: URL to the product image.
-   `availability`: inventory status.
-   `order_channel`: "warehouse_only", "online_only", or "any".

## Project Structure

-   `costco_scraper.py`: Core scraping logic, API interaction, and CLI entry point.
-   `costco_gui.py`: Tkinter/ttkbootstrap GUI wrapper.
-   `urls_part1.json`, `urls_part2.json`: Database of Costco warehouse URLs.
-   `costco_cookies.json`: (Generated) Stores session cookies to avoid repeated browser launches.
