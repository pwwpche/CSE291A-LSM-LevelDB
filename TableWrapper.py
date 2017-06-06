# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import ColumnManager

class TableWrapper():
    def __init__(self, name, columns):
        self.table_name = name
        self.columns = columns

        self.column_to_mem = {}
        for column in columns:
            self.column_to_mem[column] = ColumnManager(column)


    def get(self, column, key):
        return self.column_to_mem[column].get(key)

    def put(self, key, column, value):
        self.column_to_mem[column].put(key, value)

    def getAll(self, column):
        return []

    def remove(self, key):
        for manager in self.column_to_mem.values():
            manager.remove(key)












