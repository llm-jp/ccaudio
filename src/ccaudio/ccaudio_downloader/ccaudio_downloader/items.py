# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AudioItem(scrapy.Item):
    """Item for storing downloaded audio data and metadata"""
    audio_url = scrapy.Field()      # URL of the audio file
    title = scrapy.Field()          # Title of the audio
    description = scrapy.Field()    # Description text
    page_url = scrapy.Field()       # Source page URL
    language = scrapy.Field()       # Language code
    audio_data = scrapy.Field()     # Raw audio bytes
    content_type = scrapy.Field()   # HTTP content-type header
