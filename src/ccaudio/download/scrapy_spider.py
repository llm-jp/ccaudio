import uuid
from typing import Any, Generator

import scrapy
from datasets import load_dataset
from scrapy import Request


class AudioSpider(scrapy.Spider):
    name = "audio_spider"
    custom_settings = {
        "CONCURRENT_REQUESTS": 16,
        "DOWNLOAD_TIMEOUT": 300,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")
        ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
        self.ds = self.ds.filter(lambda x: x["language"] in ja_items)

    def start_requests(self) -> Generator[Request, None, None]:
        for data in self.ds:
            item_id = uuid.uuid4().hex
            yield Request(
                url=data["audio_url"],
                callback=self.parse,
                meta={
                    "item_id": item_id,
                    "audio_url": data["audio_url"],
                    "title": data.get("title", ""),
                    "description": data.get("description", ""),
                    "page_url": data.get("page_url", ""),
                    "language": data.get("language", ""),
                },
                dont_filter=True,
            )

    def parse(self, response: scrapy.http.Response) -> dict[str, Any]:
        meta = response.meta
        return {
            "item_id": meta["item_id"],
            "audio_data": response.body,
            "audio_url": meta["audio_url"],
            "title": meta["title"],
            "description": meta["description"],
            "page_url": meta["page_url"],
            "language": meta["language"],
            "content_type": response.headers.get("Content-Type", b"").decode("utf-8"),
        }