#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2019-12-25 15:30:29
# Project: suumo_tokyo_new_rent

import re

from pyspider.database.mysql.mysqldb import SQL
from pyspider.libs.base_handler import *


class Handler(BaseHandler):
    crawl_config = {
        'itag': 'v0.1',
        'proxy': '127.0.0.1:1087'
    }

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://suumo.jp/chintai/__JJ_FR301FC001_arz1030z2bsz1040z2tcz10401303z2tcz10401304.html',
                   callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            if re.match("^https://suumo.jp/chintai/(jnc|bc)", each.attr.href):
                self.crawl(each.attr.href, callback=self.detail_page)
        self.crawl(response.doc('div.pagination.pagination_set-nav > p > a').attr.href, callback=self.index_page)

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "name": response.doc('.section_h1-header-title').text(),
            "fee": response.doc('.property_view_note-emphasis').text(),
            "infos": ','.join([x.text() for x in response.doc('.property_view_table-body').items()])
        }

    def on_result(self, result):
        if not result or not result['url']:
            return
        sql = SQL('127.0.0.1', 'root', 'xxx', 'spiderdb')
        sql.insert('r_suumo_tokyo_new_rent', **result)
