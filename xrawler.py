# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 11:07 AM
# @Author  : Simpson
# @File    : xrawler.py

import asyncio
import aiohttp
from aiohttp import web
from lxml import etree
import logging
import pandas as pd

logger = logging.Logger(__name__)


class NewList(list):
    replace_str = ['\r\n', '\n']

    def strip_data(self, data):
        if isinstance(data, str):
            for r in self.replace_str:
                data = data.replace(r, '')
            data = data.strip()
        return data

    def pop(self, index: int = -1):
        try:
            value = super(NewList, self).pop(index)
        except IndexError as e:
            return 'None'
        return self.strip_data(value)

    def __iter__(self):
        it = super(NewList, self).__iter__()
        while True:
            data = next(it)
            yield self.strip_data(data)

    def __getitem__(self, item):
        value = super(NewList, self).__getitem__(item)
        return self.strip_data(value)


class AsyncCrawler(object):
    def __init__(self):
        self._current_url = None
        self.clear()

    async def get_content(self, url: str, method='GET', headers=None, **kwargs) -> bytes:
        print(url)
        async with aiohttp.ClientSession(headers=headers) as session:
            response = await session.get(url, ssl=False)
            if response.status == 200:
                content = await response.read()
                return content
            elif response.status == 404:
                logger.warning('HTTP响应{}：{}'.format(response.status, url))
                raise web.HTTPNotFound()
            else:
                logger.warning('HTTP响应{}：{}'.format(response.status, url))
                raise web.HTTPServerError()

    def extract_data(self, content: str, rule: str or dict) -> list or dict:
        html = etree.HTML(content)
        if isinstance(rule, str):
            data = self._html_xpath(html, rule)
        elif isinstance(rule, dict):
            data = dict()
            for name, r in rule.items():
                if isinstance(r, list):
                    info = [self._html_xpath(html, i).pop(0) for i in r]
                    info = '/'.join(info)
                else:
                    info = self._html_xpath(html, r).pop(0)
                data[name] = info
            data['url'] = self._current_url
            data = [data]
        else:
            raise TypeError
        return data

    def _html_xpath(self, html, rule: str) -> list:
        return NewList(html.xpath(rule))

    def _json_xpath(self, json: dict, rule: str):
        pass

    async def get_data(self, url, rule, charset, **kwargs):
        content = await self.get_content(url, **kwargs)
        self._current_url = url
        if rule is None:
            self.urls.append(self._current_url)
            self.data.append(content)
        else:
            self.urls.append(self._current_url)
            content = content.decode(charset)
            data = self.extract_data(content, rule)
            self.data.extend(data)


    def add(self, url, rule=None, charset='utf-8', **kwargs):
        logger.info('add url:{}'.format(url))
        future = asyncio.ensure_future(self.get_data(url, rule, charset, **kwargs))
        self.tasks.append(future)

    def run(self, loop: asyncio.AbstractEventLoop=None, close_after_complete=False):
        tasks = self.tasks
        self.clear()
        if not loop:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(tasks))
        else:
            loop.run_until_complete(self.tasks)
        if close_after_complete:
            loop.close()
        return self.data

    def clear(self):
        self.tasks = NewList()
        self.data = NewList()
        self.urls = NewList()

# 功能完善：
# Json格式提取
# 无需提取的add
# 保存数据

if __name__ == '__main__':
    url = 'http://sz.esf.fang.com/house/i3{}/'
    rule = '//div[@class="shop_list shop_list_4"]/dl/dd/h4/a/@href'
    info_rule = {
        'title': '//div[@id="lpname"]/h1/text()',
        'house_type': ['//div[@class="zf_new_left floatl"]/div/div/div[4]/span[@class="rcont"]/text()',
                       '//div[@class="xfline"]//div[@class="left"]/span[4]/text()'],
        'area': '//div[@class="xfline"]//div[@class="left"]/span[3]/text()',
        'value': '//span[@class="zf_mianji"]/b/text()',
        'building_at': '//div[@class="zf_new_left floatl"]/div/div/div[1]/span[@class="rcont"]/text()',
        'neighbourhood': '//span[@class="zf_xqname"]/text()',
        'district': ['//div[@class="s4Box"]/a/text()',
                  '//a[@id="kesfsfbxq_C03_07"]/text()',
                  '//a[@id="kesfsfbxq_C03_08"]/text()']
    }
    test = AsyncCrawler()
    for i in range(1, 3):
        url1 = url.format(i)
        test.add(url1, rule, charset='gbk')
    data = test.run()
    for url in data:
        url = 'http://sz.esf.fang.com{}'.format(url)
        test.add(url, info_rule)
    info = test.run()
    df = pd.DataFrame(info)
    df.to_csv('sz.csv')