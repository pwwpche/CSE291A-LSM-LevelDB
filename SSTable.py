# -*- coding: utf-8 -*-
from pybloom import BloomFilter
import pickle
import json


class SSTable():
    def __init__(self, name):
        self.bloom_filter = BloomFilter(capacity=1000)
        self.data = {}
        self.name = name

    def put(self, key, value):
        self.data[key] = value
        self.bloom_filter.add(key)

    def has_key(self, key):
        return key in self.bloom_filter

    def dump(self):
        with open(self.name + "_sstable_bloom.dat", "wb") as file:
            pickle.dump(self.bloom_filter, file, pickle.HIGHEST_PROTOCOL)
        with open(self.name + "_sstable_data.dat", "wb") as file:
            pickle.dump(json.dumps(self.data), file, pickle.HIGHEST_PROTOCOL)

    def load(self):
        with open(self.name + "_sstable_bloom.dat", "wb") as file:
            pickle.load(self.bloom_filter, file, pickle.HIGHEST_PROTOCOL)
        with open(self.name + "_sstable_data.dat", "wb") as file:
            pickle.load(self.data, file, pickle.HIGHEST_PROTOCOL)
        self.data = json.loads(self.data)
