# -*- coding: utf-8 -*-

class ImmutableTable():
    def __init__(self, mem):
        self.mem = mem

    def hasKey(self, key):
        return key in self.mem

    def get(self, key):
        return self.mem[key]
