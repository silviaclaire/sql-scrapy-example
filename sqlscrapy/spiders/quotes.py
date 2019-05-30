# -*- coding: utf-8 -*-
import json

import scrapy
from sqlscrapy.items import QuoteItem


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = [
        'http://quotes.toscrape.com/page/1/',
    ]

    def parse(self, response):
        for quote in response.css('div.quote'):
            item = QuoteItem()
            item['text'] = quote.css('span.text::text').get()
            item['author'] = quote.css('span small::text').get()
            item['tags'] = quote.css('div.tags a.tag::text').getall()
            yield item
        next_page = response.css('li.next a::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)
