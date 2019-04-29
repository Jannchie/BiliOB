# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from mail import mailer


class SiteItem(scrapy.Item):
    region_count = scrapy.Field()
    all_count = scrapy.Field()
    web_online = scrapy.Field()
    play_online = scrapy.Field()


class TagItem(scrapy.Item):
    tag_id = scrapy.Field()
    tag_name = scrapy.Field()
    use = scrapy.Field()
    atten = scrapy.Field()
    ctime = scrapy.Field()


class BangumiItem(scrapy.Item):
    title = scrapy.Field()
    tag = scrapy.Field()
    cover = scrapy.Field()
    square_cover = scrapy.Field()
    is_finish = scrapy.Field()
    is_started = scrapy.Field()
    newest_ep_index = scrapy.Field()
    data = scrapy.Field()


class BangumiOrDonghuaItem(scrapy.Item):
    title = scrapy.Field()
    tag = scrapy.Field()
    cover = scrapy.Field()
    square_cover = scrapy.Field()
    is_finish = scrapy.Field()
    is_started = scrapy.Field()
    newest_ep_index = scrapy.Field()
    data = scrapy.Field()
    collection = scrapy.Field()


class VideoAndAuthorItem(scrapy.Item):
    mid = scrapy.Field()
    name = scrapy.Field()
    face = scrapy.Field()
    official = scrapy.Field()
    sex = scrapy.Field()
    data_video = scrapy.Field()
    data_author = scrapy.Field()
    level = scrapy.Field()
    focus = scrapy.Field()
    pts = scrapy.Field()
    c_fans = scrapy.Field()
    c_attention = scrapy.Field()
    c_archive = scrapy.Field()
    c_article = scrapy.Field()
    c_archive_view = scrapy.Field()
    c_article_view = scrapy.Field()
    c_datetime = scrapy.Field()
    channel = scrapy.Field()
    aid = scrapy.Field()
    datetime = scrapy.Field()
    author = scrapy.Field()
    data = scrapy.Field()
    subChannel = scrapy.Field()
    title = scrapy.Field()
    mid = scrapy.Field()
    pic = scrapy.Field()
    current_view = scrapy.Field()
    current_favorite = scrapy.Field()
    current_danmaku = scrapy.Field()
    current_coin = scrapy.Field()
    current_share = scrapy.Field()
    current_like = scrapy.Field()
    current_datetime = scrapy.Field()


class VideoItem(scrapy.Item):
    object_id = scrapy.Field()
    channel = scrapy.Field()
    aid = scrapy.Field()
    datetime = scrapy.Field()
    author = scrapy.Field()
    data = scrapy.Field()
    subChannel = scrapy.Field()
    title = scrapy.Field()
    mid = scrapy.Field()
    pic = scrapy.Field()
    current_view = scrapy.Field()
    current_favorite = scrapy.Field()
    current_danmaku = scrapy.Field()
    current_coin = scrapy.Field()
    current_share = scrapy.Field()
    current_like = scrapy.Field()
    current_datetime = scrapy.Field()


class AuthorItem(scrapy.Item):
    object_id = scrapy.Field()
    mid = scrapy.Field()
    name = scrapy.Field()
    face = scrapy.Field()
    official = scrapy.Field()
    sex = scrapy.Field()
    data = scrapy.Field()
    level = scrapy.Field()
    focus = scrapy.Field()
    pts = scrapy.Field()
    c_fans = scrapy.Field()
    c_attention = scrapy.Field()
    c_archive = scrapy.Field()
    c_article = scrapy.Field()
    c_archive_view = scrapy.Field()
    c_article_view = scrapy.Field()
    c_datetime = scrapy.Field()


class RankItem(scrapy.Item):
    title = scrapy.Field()
    author = scrapy.Field()
    aid = scrapy.Field()
    pts = scrapy.Field()
    mid = scrapy.Field()
    channel = scrapy.Field()


class VideoOnline(scrapy.Item):
    title = scrapy.Field()
    author = scrapy.Field()
    data = scrapy.Field()
    aid = scrapy.Field()
    subChannel = scrapy.Field()
    channel = scrapy.Field()


class VideoWatcherItem(scrapy.Item):
    mid = scrapy.Field()
    aid = scrapy.Field()
    channels = scrapy.Field()
