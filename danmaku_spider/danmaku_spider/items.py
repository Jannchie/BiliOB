# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DanmakuAggregateItem(scrapy.Item):
    aid = scrapy.Field()
    p_name = scrapy.Field()
    page_number = scrapy.Field()
    word_frequency = scrapy.Field()
    danmaku_density = scrapy.Field()
    duration = scrapy.Field()
    object_id = scrapy.Field()
