import costco_scraper


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self._responses = responses
        self.calls = 0

    def get(self, *args, **kwargs):
        response = self._responses[self.calls]
        self.calls += 1
        return response


def test_paginate_api_collects_all_pages(tmp_path):
    payload_page1 = {"response": {"numFound": 3, "docs": [{"id": 1}, {"id": 2}]}}
    payload_page2 = {"response": {"numFound": 3, "docs": [{"id": 3}]}}

    session = FakeSession([FakeResponse(200, payload_page1), FakeResponse(200, payload_page2)])
    config = costco_scraper.ScraperConfig(output_dir=tmp_path, cookies_file=tmp_path / "cookies.json", page_rows=2)

    docs = costco_scraper.paginate_api(
        session,
        "https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_navigation?start=0&rows=2",
        {},
        config,
    )

    assert [doc["id"] for doc in docs] == [1, 2, 3]


def test_paginate_api_handles_empty_docs(tmp_path):
    payload = {"response": {"numFound": 0, "docs": []}}
    session = FakeSession([FakeResponse(200, payload)])
    config = costco_scraper.ScraperConfig(output_dir=tmp_path, cookies_file=tmp_path / "cookies.json", page_rows=2)

    docs = costco_scraper.paginate_api(
        session,
        "https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_navigation?start=0&rows=2",
        {},
        config,
    )

    assert docs == []
