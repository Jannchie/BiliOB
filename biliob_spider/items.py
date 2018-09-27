# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class VideoItem(scrapy.Item):
    channel = scrapy.Field()
    aid = scrapy.Field()
    datetime = scrapy.Field()
    author = scrapy.Field()
    view = scrapy.Field()
    favorite = scrapy.Field()
    coin = scrapy.Field()
    share = scrapy.Field()
    like = scrapy.Field()
    danmaku = scrapy.Field()
    dislike = scrapy.Field()
    subChannel = scrapy.Field()
    title = scrapy.Field()


class AuthorItem(scrapy.Item):
    mid = scrapy.Field()
    name = scrapy.Field()
    face = scrapy.Field()
    official = scrapy.Field()
    sex = scrapy.Field()
    data = scrapy.Field()
    level = scrapy.Field()

