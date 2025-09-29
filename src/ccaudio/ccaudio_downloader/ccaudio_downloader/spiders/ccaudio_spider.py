import logging

import scrapy
from datasets import load_dataset
from scrapy.utils.project import get_project_settings

from ..items import AudioItem

logger = logging.getLogger(__name__)


class CcaudioSpiderSpider(scrapy.Spider):
    name = "ccaudio_spider"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset = None

    async def start(self):
        """Load HuggingFace dataset and yield requests for each audio URL"""
        settings = get_project_settings()
        dataset_name = settings.get("DATASET_NAME")

        logger.info(f"Loading {dataset_name} dataset from HuggingFace...")

        # Load the dataset
        self.dataset = load_dataset(dataset_name, split="train")

        # Filter for Japanese content
        ja_items = ["ja", "ja_JP", "ja-jp", "ja-JP"]
        self.dataset = self.dataset.filter(lambda x: x["language"] in ja_items)

        logger.info(f"Found {len(self.dataset)} Japanese audio items")

        # Yield requests for each audio URL
        for i, data in enumerate(self.dataset):
            audio_url = data.get("audio_url")
            if audio_url:
                yield scrapy.Request(
                    url=audio_url,
                    callback=self.parse,
                    meta={
                        "index": i,
                        "title": data.get("title", ""),
                        "description": data.get("description", ""),
                        "page_url": data.get("page_url", ""),
                        "language": data.get("language", ""),
                    },
                    dont_filter=True,
                    errback=self.errback_httpbin,
                )

    def parse(self, response):
        """Parse the audio response and yield AudioItem"""
        meta = response.meta

        # Create the audio item
        item = AudioItem()
        item["audio_url"] = response.url
        item["title"] = meta.get("title", "")
        item["description"] = meta.get("description", "")
        item["page_url"] = meta.get("page_url", "")
        item["language"] = meta.get("language", "")
        item["audio_data"] = response.body
        item["content_type"] = response.headers.get("Content-Type", b"").decode(
            "utf-8", errors="ignore"
        )

        logger.info(
            f"Downloaded audio {meta.get('index')}: {meta.get('title')[:50]}..."
        )

        yield item

    def errback_httpbin(self, failure):
        """Handle download errors"""
        request = failure.request
        logger.error(f"Failed to download {request.url}: {failure.value}")

        # Could implement retry logic here if needed
        # For now, just log the error and continue
