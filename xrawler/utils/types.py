# -*- coding: utf-8 -*-
# @Time    : 2019/3/26 11:08 AM
# @Author  : Simpson
# @File    : types.py

import config


class NewList(list):
    replace_str = config.str_replace_list

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