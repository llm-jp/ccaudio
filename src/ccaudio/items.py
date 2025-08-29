import scrapy


class AudioItem(scrapy.Item):
    audio_url = scrapy.Field()
    title = scrapy.Field()
    descriptions = scrapy.Field()
    page_url = scrapy.Field()
    language = scrapy.Field()
    audio_data = scrapy.Field()
