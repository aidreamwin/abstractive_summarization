# -*- coding: utf-8 -*-

from bert_serving.client import BertClient


class Embedding(object):
    """docstring for Embedding"""

    def __init__(self, ip, port):
        # 初始化TF Serving
        self.bc = BertClient(ip=ip, port=port)

    # 获取向量
    def get_embedding(self, keywords):
        for v in keywords:
            if keywords == "" or len(keywords) <1:
                print("非法参数，参数不能有空值")
                return []
        ems = self.bc.encode(keywords)
        return ems
