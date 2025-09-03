from collections.abc import AsyncIterator

import scrapy
from datasets import load_dataset
from scrapy import Request
from scrapy.http import Response

from ccaudio.download.items import AudioItem


class CCAudioSpider(scrapy.Spider):
    name = "download_ccaudio_spider"

    def __init__(self) -> None:
        super(CCAudioSpider, self).__init__()

        self.ds = load_dataset("llm-jp/cc-audio-2025-18-rss", split="train")
        ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
        self.ds = self.ds.filter(lambda x: x["language"] in ja_items)

    async def start(self) -> AsyncIterator[Request]:
        for data in self.ds:
            assert isinstance(data, dict)
            yield Request(
                url=data["audio_url"],
                callback=self.parse,
                meta={
                    "audio_url": data["audio_url"],
                    "title": data.get("title", ""),
                    "description": data.get("description", ""),
                    "page_url": data.get("page_url", ""),
                    "language": data.get("language", ""),
                },
                dont_filter=True,
            )

    def parse(self, response: Response) -> AudioItem:
        meta = response.meta
        content_type = response.headers.get("Content-Type", b"")
        assert content_type is not None
        return AudioItem(
            audio_url=meta["audio_url"],
            title=meta["title"],
            description=meta["description"],
            page_url=meta["page_url"],
            language=meta["language"],
            audio_data=response.body,
            content_type=content_type.decode("utf-8"),
        )
