# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from pybloom import BloomFilter
import pickle
import json

class TableWrapper:
    def __init__(self, name, columns):
        self.table_name = name
        self.columns = columns

        self.column_to_mem = {}
        for column in columns:
            self.column_to_mem[column] = InMemManager(column)


    def get(self, column, key):
        return self.column_to_mem[column].get(key)

    def put(self, key, column, value):
        self.column_to_mem[column].put(key, value)

    def getAll(self, column):
        return []

    def remove(self, key):
        for manager in self.column_to_mem.values():
            manager.remove(key)

class InMemManager:
    def __init__(self, column_name):
        self.column_name = column_name
        self.mem = {}

    def get(self, key):


    def put(self, key, value):

    def remove(self, key):




class InMemTable:
    def __init__(self):
        self.mem = {}
    def get(self, key):
        return self.mem.get(key, None)

    def put(self, key, value):
        self.mem[key] = value

    def remove(self, key):
        del self.mem[key]

class ImmutableTable:
    def __init__(self, mem):
        self.mem = mem

    def hasKey(self, key):
        return key in self.mem
    d



class SSTable:
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


