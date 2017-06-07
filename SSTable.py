# -*- coding: utf-8 -*-
from pybloom import BloomFilter
import pickle
import json
import huffman
import os
from Queue import PriorityQueue
import collections
import math
import gzip
import shutil

class SSTable:
    def __init__(self, name):
        self.__name = name
        self.locked = False
        self.__key_file = {}
        self.__meta = {
            "max_minor": 10,
            "max_major": 10,
            "minor": 0,
            "major": 0,
            "gzip_compression": True,
            "dict_compression": True,
            "block_size": 10
        }
        self.__cur_file = {}
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
            data = self.__fetch_by_key(self.__key_file[key], key)
            return data[key]
        if not self.contains_key(key):
            return None
        # data = self.__load_ordered_data(self.__key_file[key])
        # return data[key]
        return self.__fetch_by_key(self.__key_file[key], key)

    def getnumber(self):
        self.locked = True
        total_size = 0
        for i in range(self.__meta["minor"]):
            total_size += len(self.__load_index(self.__name + str(i) + "_minor"))
        for i in range(self.__meta["major"]):
            total_size += len(self.__load_index(self.__name + str(i) + "_minor"))
        self.__cur_file = {"type": "major" if self.__meta["minor"] == 0 else "minor"}
        self.__cur_file["index"] = self.__meta[self.__cur_file['type']] - 1
        self.__cur_file["pos"] = 0
        return math.ceil(total_size * 1.0 / self.__meta["block_size"])

    def getfile(self, index):
        if self.__cur_file['type'] == "major" and self.__cur_file['index'] == -1:
            self.locked = False
            return None
        data = self.__load_ordered_array(self.__name + str(self.__cur_file['index']) + "_" + self.__cur_file['type'])
        start = self.__cur_file["pos"]
        end = self.__cur_file["pos"] + self.__meta["block_size"]
        ret_data = collections.OrderedDict(data[start:end])
        self.__cur_file["pos"] = end

        if self.__cur_file["pos"] >= len(data):
            self.__cur_file["pos"] = 0
            self.__cur_file["index"] -= 1
            if self.__cur_file["index"] == -1 and self.__cur_file["type"] == "minor":
                self.__cur_file["type"] = "major"
                self.__cur_file["index"] = self.__meta["minor"] - 1
        return ret_data

    def store(self, mem):
        if self.locked:
            return
        mem = [(k, mem[k]) for k in mem]
        mem = sorted(mem, key=lambda t: t[0])
        table_id = self.__meta["minor"]
        self.__dump_table(self.__name + str(table_id) + "_minor", mem)
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
                q.put((partitions[idx][pos + 1][0], idx, pos + 1))
        return merged

    def __dump_table(self, file_name, mem_list):
        ft = BloomFilter(capacity=1000)
        for key in [k for (k, v) in mem_list]:
            ft.add(key)
        key_pos = []
        with open(file_name + "_sstable_bloom.dat", "wb") as openfile:
            pickle.dump(ft, openfile, pickle.HIGHEST_PROTOCOL)
        if self.__meta['dict_compression']:
            comp_dict, mem_list = self.__huffman_compression(mem_list)
            with open(file_name + "_sstable_comp_dict.dat", "wb") as openfile:
                pickle.dump(comp_dict, openfile, pickle.HIGHEST_PROTOCOL)
        with open(file_name + "_sstable_data.dat", "wb") as openfile:
            for (k, v) in mem_list:
                key_pos.append((k, openfile.tell()))
                pickle.dump((k, v), openfile, pickle.HIGHEST_PROTOCOL)
        if self.__meta['gzip_compression']:
            self.__gzip_compression(file_name)
        with open(file_name + "_sstable_keys.dat", "wb") as openfile:
            pickle.dump(key_pos, openfile, pickle.HIGHEST_PROTOCOL)

    def __load_index(self, file_name):
        with open(file_name + "_sstable_keys.dat", "rb") as openfile:
            s = pickle.load(openfile)
            return collections.OrderedDict(s)

    def __load_ordered_data(self, file_name):
        arr = self.__load_ordered_array(file_name)
        data = collections.OrderedDict(arr)
        return data

    def __fetch_by_key(self, file_name, key):
        key_idx = self.__load_index(file_name)
        pos = key_idx[key]
        if self.__meta['gzip_compression']:
            self.__gzip_decompression(file_name)
        with open(file_name + "_sstable_data.dat", "rb") as openfile:
            openfile.seek(pos)
            (key, val) = pickle.load(openfile)
            if self.__meta['dict_compression']:
                with open(file_name + "_sstable_comp_dict.dat", "rb") as openfile:
                    comp_dict = pickle.load(openfile)
                    val = comp_dict[val]
            return key, val

    def __load_ordered_array(self, file_name):
        data = []
        if self.__meta['gzip_compression']:
            self.__gzip_decompression(file_name)
        with open(file_name + "_sstable_data.dat", "rb") as openfile:
            while 1:
                try:
                    data.append(pickle.load(openfile))
                except EOFError:
                    break
        if self.__meta['dict_compression']:
            with open(file_name + "_sstable_comp_dict.dat", "rb") as openfile:
                comp_dict = pickle.load(openfile)
            uncompressed_data = []
            for (key, value) in data:
                uncompressed_data.append((key, comp_dict[value]))
            data = uncompressed_data
        return data

    def __load_filter(self, file_name):
        with open(file_name + "_sstable_bloom.dat", "rb") as openfile:
            bf = pickle.load(openfile)
            return bf

    def __huffman_compression(self, ordered_data):
        comp_dict = {}
        for (key, value) in ordered_data:
            if value not in comp_dict:
                comp_dict[value] = 0
            comp_dict[value] += 1
        comp_dict = huffman.codebook([(k, comp_dict[k]) for k in comp_dict])
        comp_data = [(k, comp_dict[v]) for (k, v) in ordered_data]
        comp_dict = {comp_dict[k]: k for k in comp_dict}
        return comp_dict, comp_data

    def __gzip_compression(self, file_name):
        with open(file_name + "_sstable_data.dat", 'rb') as f_in, gzip.open(file_name + "_sstable_data.dat.gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    def __gzip_decompression(self, file_name):
        with gzip.open(file_name + "_sstable_data.dat.gz", 'rb') as f_in, open(file_name + "_sstable_data.dat", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)