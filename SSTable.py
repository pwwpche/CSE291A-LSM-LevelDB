# -*- coding: utf-8 -*-
from pybloom import BloomFilter
import pickle
import json
from sets import Set
import huffman
import os
from Queue import PriorityQueue
import collections
import math

class SSTable():

    def __init__(self, name):
        self.__name = name
        self.locked = False
        self.__key_file = {}
        self.__meta = {
            "max_minor": 10,
            "max_major": 10,
            "minor": 0,
            "major": 0,
            "block_size": 10
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
            data = self.__load_ordered_data(self.__key_file[key])
            return data[key]
        if not self.contains_key(key):
            return None
        data = self.__load_ordered_data(self.__key_file[key])
        return data[key]

    def getnumber(self):
        self.locked = True
        totSize = 0
        for i in range(self.__meta["minor"]):
            totSize += len(self.__load_index(self.__name + str(i) + "_minor"))
        for i in range(self.__meta["major"]):
            totSize += len(self.__load_index(self.__name + str(i) + "_minor"))
        self.cur_file = {}
        self.cur_file["type"] = "major" if self.__meta["minor"] == 0 else "minor"
        self.cur_file["index"] = self.__meta[self.cur_file['type']] - 1
        self.cur_file["pos"] = 0
        return math.ceil(totSize * 1.0 / self.__meta["block_size"])


    def getfile(self, index):
        if self.cur_file['type'] == "major" and self.cur_file['index'] == -1:
            self.locked = False
            return None
        data = self.__load_ordered_array(self.__name + str(self.cur_file['index']) + "_" + self.cur_file['type'])
        start = self.cur_file["pos"]
        end = self.cur_file["pos"] + self.__meta["block_size"]
        ret_data = collections.OrderedDict(data[start:end])
        self.cur_file["pos"] = end

        if self.cur_file["pos"] >= len(data):
            self.cur_file["pos"] = 0
            self.cur_file["index"] -= 1
            if self.cur_file["index"] == -1 and self.cur_file["type"] == "minor":
                self.cur_file["type"] = "major"
                self.cur_file["index"] = self.__meta["minor"] - 1
        return ret_data


    def store(self, mem):
        if self.locked:
            return
        mem = collections.OrderedDict(mem)

        table_id = self.__meta["minor"]
        self.__dump_table(self.__name + str(table_id) + "_minor", mem.items())
        self.__meta["minor"] += 1
        # Update key-file lookup table
        for key in self.__key_file:
            if key in mem:
                self.__key_file[key] = self.__name + str(table_id) + "_minor"

        # Combine all minor compactions into one major compaction
        if self.__meta["minor"] == self.__meta["max_minor"]:
            table_id = self.__meta["major"]
            partitions = []
            # Collect minor compactions and combine them to a dict
            for i in range(self.__meta["minor"]):
                partitions.append(self.__load_ordered_array(self.__name + str(i) + "_minor"))
            # Merge partitions
            major_mem = self.__merge(partitions)

            self.__dump_table(self.__name + str(table_id) + "_major", major_mem)
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

    def __merge(self, partitions):
        partitions.reverse()
        merged = []
        q = PriorityQueue()
        for i in range(len(partitions)):
            if len(partitions[i]) > 0:
                q.put((partitions[i][0][0], i, 0))
        while not q.empty():
            (key, idx, pos) = q.get()
            while not q.empty() and q.queue[0][0] == key:
                (k, i, p) = q.get()
                if p + 1 < len(partitions[i]):
                    q.put((partitions[i][p + 1][0], i, p + 1))
            merged.append((key, partitions[idx][pos][1]))
            if pos + 1 < len(partitions[idx]):
                q.put((partitions[idx][pos+1][0], idx, pos + 1))
        return merged


    def __dump_table(self, file_name, mem_list):
        ft = BloomFilter(capacity=1000)
        keys = [k for (k,v) in mem_list]
        for key in keys:
            ft.add(key)
        with open(file_name + "_sstable_bloom.dat", "wb") as openfile:
            pickle.dump(ft, openfile, pickle.HIGHEST_PROTOCOL)
        with open(file_name + "_sstable_keys.dat", "wb") as openfile:
            pickle.dump(keys, openfile, pickle.HIGHEST_PROTOCOL)
        with open(file_name + "_sstable_data.dat", "wb") as openfile:
            pickle.dump(mem_list, openfile, pickle.HIGHEST_PROTOCOL)

    def __load_index(self, file_name):
        s = Set()
        with open(file_name + "_sstable_keys.dat", "rb") as openfile:
            s = pickle.load(openfile)
        return s

    def __load_ordered_data(self, file_name):
        arr = self.__load_ordered_array(file_name)
        data = collections.OrderedDict(arr)
        return data

    def __load_ordered_array(self, file_name):
        data = []
        with open(file_name + "_sstable_data.dat", "rb") as openfile:
            data = pickle.load(openfile)
        if os.path.isfile(file_name + "_sstable_comp_dict.dat"):
            with open(file_name + "_sstable_comp_dict.dat", "rb") as openfile:
                comp_dict = pickle.load(openfile)
            for key in data:
                data[key] = comp_dict[data[key]]
        return data

    def __load_filter(self, file_name):
        bf = BloomFilter(capacity=1000)
        with open(file_name + "_sstable_bloom.dat", "rb") as openfile:
            bf = pickle.load(openfile)
        return bf

    def __huffman_compression(self, file_name):
        if os.path.isfile(file_name + "_sstable_comp_dict.dat"):
            return
        data = self.__load_ordered_data(file_name)
        comp_dict = {}
        for key in data:
            if data[key] not in comp_dict:
                comp_dict[data[key]] = 0
            comp_dict[data[key]] += 1
        comp_dict = huffman.codebook([(k, comp_dict[k]) for k in comp_dict])

        for key in data:
            data[key] = comp_dict[data[key]]
        with open(file_name + "_sstable_comp_dict.dat", "wb") as openfile:
            pickle.dump(comp_dict, openfile, pickle.HIGHEST_PROTOCOL)
        with open(file_name + "_sstable_data.dat", "wb") as openfile:
            pickle.dump(data, openfile, pickle.HIGHEST_PROTOCOL)
