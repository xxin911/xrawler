# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 11:07 AM
# @Author  : Simpson
# @File    : crawler.py

import abc
import asyncio
import json
import logging
import re

import aiohttp
from aiohttp import web

import config
from utils.types import NewList
from core.extract import JsonExtract, HtmlExtract

logger = logging.Logger(__name__)


class Crawler(object):
    @abc.abstractmethod
    def add(self, url: str, rule, headers=None, verify=False,
            timeout=None, charset='utf-8', **kwargs):
        '''添加爬取规则'''

    @abc.abstractmethod
    def run(self):
        '''运行爬虫'''


class SyncCrawler(Crawler):
    pass


class AsyncCrawler(Crawler):
    def __init__(self, headers=None, cookies=None, timeout=None):
        self._current_url = None
        self.session = aiohttp.ClientSession(
            headers=headers, cookies=cookies, auth=None, conn_timeout=timeout)
        self.clear()

    async def get_content(self, url: str, method='get',
                          headers=None, **kwargs) -> bytes:
        print(url)
        if method not in config.allow_methods:
            logger.error('method:"{}" not allow'.format(method))
            raise Exception('method:"{}" not allow'.format(method))
        request_method = getattr(self.session, method.lower(), None)
        if request_method is None:
            logger.error('method:"{}" not found'.format(method))
            raise Exception('method:"{}" not found'.format(method))
        response = await request_method(url, headers=headers, **kwargs)
        content = await response.read()
        return content

    def extract_data(self, text: str, rule) -> list:
        try:
            data = JsonExtract.get_data(text, rule)
        except json.JSONDecodeError as e:
            data = HtmlExtract.get_data(text, rule)
        return data

    async def get_data(self, url, rule, charset, **kwargs):
        content = await self.get_content(url, **kwargs)
        self._current_url = url
        self.urls.append(self._current_url)
        if charset is not None:
            content = content.decode(charset)
        data = self.extract_data(content, rule)
        if isinstance(data, list):
            self.data.extend(data)
        else:
            self.data.append(data)

    def add(self, url, rule=None, charset='utf-8', **kwargs):
        logger.info('add url:{}'.format(url))
        obj = self.get_data(url, rule, charset, **kwargs)
        self._add_future(obj)

    def _add_future(self, obj):
        future = asyncio.ensure_future(obj)
        self.tasks.append(future)

    def run(self):
        tasks = self.tasks
        self.clear()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        return self.data

    def clear(self):
        self.tasks = NewList()
        self.data = NewList()
        self.urls = NewList()

# 功能完善：
# Json格式提取
# 无需提取的add，用于图片保存之类
# 保存数据

if __name__ == '__main__':
    url = 'http://sz.esf.fang.com/house/i3{}/'
    rule = '//div[@class="shop_list shop_list_4"]/dl/dd/h4/a/@href'
    info_rule = {
        'title': '//div[@id="lpname"]/h1/text()/[0]',
        'house_type': ['//div[@class="zf_new_left floatl"]/div/div/div[4]/span[@class="rcont"]/text()/[0]',
                       '//div[@class="xfline"]//div[@class="left"]/span[4]/text()/[0]'],
        'area': '//div[@class="xfline"]//div[@class="left"]/span[3]/text()/[0]',
        'value': '//span[@class="zf_mianji"]/b/text()/[0]',
        'building_at': '//div[@class="zf_new_left floatl"]/div/div/div[1]/span[@class="rcont"]/text()/[0]',
        'neighbourhood': '//span[@class="zf_xqname"]/text()/[0]',
        'district': ['//div[@class="s4Box"]/a/text()/[0]',
                  '//a[@id="kesfsfbxq_C03_07"]/text()/[0]',
                  '//a[@id="kesfsfbxq_C03_08"]/text()/[0]']
    }
    test = AsyncCrawler()
    for i in range(3, 4):
        url1 = url.format(i)
        test.add(url1, rule, charset='gbk', raise_for_status=True, verify_ssl=False)
    data = test.run()
    print(data)
    for url in data:
        url = 'http://sz.esf.fang.com{}'.format(url)
        test.add(url, info_rule, raise_for_status=True, verify_ssl=False)
        break
    info = test.run()
    print(info)

    # test.add('http://devtest8.buildingqm.com/uc/user/login/?user_name=kentestgrp10&password=12345678&remember_me=1', {'group_id': '//data/group_id'})
    # data = test.run()
    # print(data)
