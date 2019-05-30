# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs
import sqlite3
from datetime import datetime
from collections import OrderedDict


class JsonWithEncodingPipeline(object):
    def __init__(self):
        self.file = codecs.open('data_utf8.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        line = json.dumps(OrderedDict(item), ensure_ascii=False, sort_keys=False) + "\n"
        self.file.write(line)
        return item

    def close_spider(self, spider):
        self.file.close()


class SqlPipeline(object):
    def __init__(self):
        self.conn = sqlite3.connect('quotes.db')
        self.c = self.conn.cursor()
        self.c.execute("CREATE TABLE IF NOT EXISTS quotes (text text, author text, tags text)")

    def process_item(self, item, spider):
        self.c.execute(
                "INSERT INTO quotes VALUES (?,?,?)",
                (item['text'], item['author'], json.dumps(item['tags']).encode('utf8')))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        self.conn.close()
