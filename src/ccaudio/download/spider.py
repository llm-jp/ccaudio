import scrapy
from scrapy.http import Response

from ccaudio.download.items import AudioItem


class DownloadCCAudioSpider(scrapy.Spider):
    name = "download_ccaudio_spider"

    def parse(self, response: Response) -> AudioItem:
        meta = response.meta
        return AudioItem(
            audio_url=meta["audio_url"],
            title=meta["title"],
            description=meta["description"],
            page_url=meta["page_url"],
            language=meta["language"],
            audio_data=response.body,
        )
