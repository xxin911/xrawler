# -*- coding: utf-8 -*-
# @Time    : 2019/3/26 11:09 AM
# @Author  : Simpson
# @File    : extract.py
import abc
import re
import logging
import json

from lxml import etree

from utils.types import NewList

logger = logging.getLogger(__name__)


class Extract(object):

    @abc.abstractclassmethod
    def extract_rule(cls, data, rule: str):
        '''extract data by rule'''

    @classmethod
    def get_data(cls, html_data, rule):
        if rule is None:
            return html_data
        if isinstance(rule, str):
            return cls.extract_rule(html_data, rule)
        elif isinstance(rule, list):
            return [cls.get_data(html_data, r) for r in rule]
        elif isinstance(rule, dict):
            return {name: cls.get_data(html_data, r)
                    for name, r in rule.items()}
        else:
            logger.error('rule<{}> not str, list or dict'.format(type(rule)))
            raise TypeError('rule<{}> not str, list or dict'.format(type(rule)))

    @classmethod
    def index(cls, index):
        if isinstance(index, int):
            return index
        elif isinstance(index, str):
            slice_tuple = index.split(':')
            if len(slice_tuple) == 1 and slice_tuple[0].isdigit():
                return int(slice_tuple[0])
            return slice(*(int(i) if i and i.isdigit() else None
                           for i in slice_tuple))
        return slice(None)


class JsonExtract(Extract):
    @classmethod
    def extract_rule(cls, json_data, rule: str):
        reg = '([^/]+){1,}'
        re_compile = re.compile(reg)
        rules = re_compile.findall(rule)
        if not rules:
            logger.error('Json rule invalid syntax: {}'.format(rule))
            raise Exception('Json rule invalid syntax: {}'.format(rule))
        json_data = json.loads(json_data)
        for rule in rules:
            if re.match('^\[.*\]$', rule):
                json_data = json_data[cls.index(rule[1:-1])]
            else:
                json_data = json_data[rule]
        return json_data


class HtmlExtract(Extract):
    @classmethod
    def extract_rule(cls, html_data, rule: str):
        reg = '^(.*?)/?(\[\d+\])?$'
        re_compile = re.compile(reg)
        rules = re_compile.findall(rule)
        if not rules:
            logger.error('Html rule invalid syntax: {}'.format(rule))
            raise Exception('Html rule invalid syntax: {}'.format(rule))
        rule, index = rules.pop()
        html = etree.HTML(html_data)
        data = NewList(html.xpath(rule))
        if index:
            data = data[cls.index(index[1:-1])]
        return data
