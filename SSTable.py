# -*- coding: utf-8 -*-
from pybloom import BloomFilter
import pickle
import json
from sets import Set


class SSTable:
    def __init__(self, name):
        self.__name = name
        self.locked = False
        self.__key_file = {}
        self.__meta = {
            "max_minor": 10,
            "minor": 0,
            "major": 0
        }
        with open("ssMeta.dat", "w+") as my_file:
            line = my_file.readline()
            if len(line) > 0:
                self.__meta = json.loads(line)

    def contains_key(self, key):
        for i in reversed(range(self.__meta["minor"])):
            if self.__check_table(self.__name + str(i) + "_minor", key):
                return True
        for i in reversed(range(self.__meta["major"])):
            if self.__check_table(self.__name + str(i) + "_major", key):
                return True
        return False

    def get(self, key):
        if key in self.__key_file:
            data = self.__load_table_data(self.__key_file[key])
            return data[key]
        if not self.contains_key(key):
            return None
        data = self.__load_table_data(self.__key_file[key])
        return data[key]

    def getnumber(self):
        return self.__meta["minor"] + self.__meta["major"]

    def getfile(self, index):
        if index < self.__meta["minor"]:
            return self.__load_table_data(self.__name + str(index) + "_minor")
        else:
            return self.__load_table_data(self.__name + str(index - self.__meta["minor"]) + "_major")

    def store(self, mem):
        if self.locked:
            return

        table_id = self.__meta["minor"]
        self.__dump_table(self.__name + table_id + "_minor", mem)
        self.__meta["minor"] += 1
        # Update key-file lookup table
        for key in self.__key_file:
            if key in mem.keys():
                self.__key_file[key] = self.__name + table_id + "_minor"
        # Combine all minor compactions into one major compaction
        if self.__meta["minor"] == self.__meta["max_minor"]:
            mem = {}
            table_id = self.__meta["major"]
            # Collect minor compactions and combine them to a dict
            for i in range(self.__meta["minor"]):
                partition = self.__load_table_data(self.__name + str(i) + "_minor")
                for key in partition.keys():
                    mem[key] = partition[key]
            self.__dump_table(self.__name + str(table_id) + "_major")
            self.__meta["major"] += 1
            self.__meta["minor"] = 0
            # Update lookup table
            for key in self.__key_file:
                if "_minor" in key:
                    self.__key_file[key] = self.__name + str(table_id) + "_major"
        # Update meta file
        with open("ssMeta.dat", "w") as my_file:
            my_file.write(json.dumps(self.__meta))

    def __check_table(self, table_name, key):
        bf = self.__load_filter(table_name)
        if key in bf:
            index = self.__load_index(table_name)
            if key in index:
                self.__key_file[key] = table_name
                return True
        return False

    def __dump_table(self, file_name, mem):
        ft = BloomFilter(capacity=1000)
        for key in mem.keys():
            ft.add(key)
        with open(file_name + "_sstable_bloom.dat", "wb") as openfile:
            pickle.dump(ft, openfile, pickle.HIGHEST_PROTOCOL)
        with open(file_name + "_sstable_keyset.dat", "wb") as openfile:
            pickle.dump(Set(mem.keys()), openfile, pickle.HIGHEST_PROTOCOL)
        with open(file_name + "_sstable_data.dat", "wb") as openfile:
            pickle.dump(json.dumps(mem), openfile, pickle.HIGHEST_PROTOCOL)

    def __load_index(self, file_name):
        s = Set()
        with open(file_name + "_sstable_keyset.dat", "wb") as openfile:
            pickle.load(s, openfile, pickle.HIGHEST_PROTOCOL)
        return s

    def __load_table_data(self, file_name):
        data = ""
        with open(file_name + "_sstable_data.dat", "wb") as openfile:
            pickle.load(data, openfile, pickle.HIGHEST_PROTOCOL)
        data = json.loads(data)
        return data

    def __load_filter(self, file_name):
        bf = BloomFilter(capacity=1000)
        with open(file_name + "_sstable_bloom.dat", "wb") as openfile:
            pickle.load(bf, openfile, pickle.HIGHEST_PROTOCOL)
        return bf
