#coding=utf-8
import scrapy
from scrapy.http import Request
from biliob_spider.items import BangumiItem
import time
import datetime


class DonghuaSpider(scrapy.spiders.Spider):
    name = "donghua"
    allowed_domains = ["bilibili.com"]
    start_urls = ["https://www.bilibili.com/ranking/bangumi/167/0/7"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.DonghuaPipeLine': 200
        }
    }

    def parse(self, response):
        detail_href = response.xpath("//div[@class='img']/a/@href").extract()

        pts = response.xpath("//div[@class='pts']/div/text()").extract()
        for (each_href, each_pts) in zip(detail_href, pts):
            yield Request(
                "https:" + each_href,
                meta={'pts': each_pts},
                callback=self.detail_parse)

    def detail_parse(self, response):
        pts = response.meta['pts']
        play = response.xpath(
            '//*[@id="app"]/div[1]/div[2]/div/div[2]/div[2]/div[1]/span[1]/em/text()'
        ).extract()[0]
        watch = response.xpath(
            '//*[@id="app"]/div[1]/div[2]/div/div[2]/div[2]/div[1]/span[2]/em/text()'
        ).extract()[0]
        danmaku = response.xpath(
            '//*[@id="app"]/div[1]/div[2]/div/div[2]/div[2]/div[1]/span[3]/em/text()'
        ).extract()[0]
        title = response.xpath(
            '//*[@id="app"]/div[1]/div[2]/div/div[2]/div[1]/span[1]/text()'
        ).extract()[0]
        tag = response.xpath('//span[@class="media-tag"]/text()').extract()
        data = {
            'danmaku': danmaku,
            'watch': watch,
            'play': play,
            'pts': int(pts),
            'datetime': datetime.datetime.now()
        }
        item = BangumiItem()
        item['tag'] = tag
        item['title'] = title
        item['data'] = data
        yield item