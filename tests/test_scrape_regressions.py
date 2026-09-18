import costco_scraper


def test_scrape_warehouse_no_cookies_and_no_refresh(monkeypatch, tmp_path):
    monkeypatch.setattr(costco_scraper, "load_cookies", lambda *_: None)

    config = costco_scraper.ScraperConfig(output_dir=tmp_path, cookies_file=tmp_path / "cookies.json")
    warehouse = {"id": "117", "name": "Oak Brook", "state": "IL"}

    result = costco_scraper.scrape_warehouse(warehouse, config=config, allow_cookie_refresh=False)

    assert result["success"] is False
    assert "No cookies found" in result["error"]


def test_scrape_warehouse_refreshes_after_empty_result(monkeypatch, tmp_path):
    calls = {"paginate": 0}

    monkeypatch.setattr(costco_scraper, "load_cookies", lambda *_: [{"name": "a", "value": "b"}])

    async def fake_refresh(_config):
        return [{"name": "fresh", "value": "cookie"}]

    monkeypatch.setattr(costco_scraper, "refresh_cookies_interactive", fake_refresh)

    def fake_paginate(*args, **kwargs):
        calls["paginate"] += 1
        if calls["paginate"] == 1:
            return []
        return [{"item_number": "123", "item_product_name": "Name"}]

    monkeypatch.setattr(costco_scraper, "paginate_api", fake_paginate)
    monkeypatch.setattr(
        costco_scraper,
        "enrich_and_save",
        lambda docs, *_args, **_kwargs: {
            "rows": len(docs),
            "output_file": str(tmp_path / "result.csv"),
            "enriched_items": 1,
            "failed_graphql_batches": 0,
        },
    )

    config = costco_scraper.ScraperConfig(output_dir=tmp_path, cookies_file=tmp_path / "cookies.json")
    warehouse = {"id": "117", "name": "Oak Brook", "state": "IL"}

    result = costco_scraper.scrape_warehouse(warehouse, config=config, allow_cookie_refresh=True)

    assert result["success"] is True
    assert result["rows"] == 1
    assert calls["paginate"] == 2
