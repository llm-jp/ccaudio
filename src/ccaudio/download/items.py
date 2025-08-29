import scrapy


class Audio(scrapy.Item):
    audio_url = scrapy.Field()
    title = scrapy.Field()
    descriptions = scrapy.Field()
    page_url = scrapy.Field()
    language = scrapy.Field()
