import costco_scraper


def test_parse_warehouse_info_valid_url():
    result = costco_scraper.parse_warehouse_info(
        "https://www.costco.com/warehouse-locations/oak-brook-il-117.html"
    )
    assert result == {
        "id": "117",
        "name": "Oak Brook",
        "state": "IL",
        "url": "https://www.costco.com/warehouse-locations/oak-brook-il-117.html",
    }


def test_parse_warehouse_info_invalid_url_returns_none():
    assert costco_scraper.parse_warehouse_info("https://www.costco.com/some-other-page") is None


def test_determine_order_channel_online_only():
    payload = {
        "catalogData": [
            {
                "attributes": [{"key": "Online Only", "value": "Online Only"}],
                "programTypes": "eCommerce",
            }
        ]
    }
    assert costco_scraper.determine_order_channel(payload) == "online_only"


def test_determine_order_channel_warehouse_only():
    payload = {
        "catalogData": [
            {
                "attributes": [{"key": "Warehouse Only", "value": "Warehouse Only"}],
                "programTypes": "InWarehouse",
            }
        ]
    }
    assert costco_scraper.determine_order_channel(payload) == "warehouse_only"


def test_normalize_doc_fallbacks_to_badges_without_graphql():
    doc = {
        "item_number": "123",
        "item_product_name": "Sample Product",
        "item_pill_attributes": ["Warehouse Only"],
    }

    row = costco_scraper.normalize_doc(doc, {}, "Oak Brook", "117")

    assert row["item_number"] == "123"
    assert row["name"] == "Sample Product"
    assert row["order_channel"] == "warehouse_only"
