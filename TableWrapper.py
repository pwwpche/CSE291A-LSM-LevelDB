# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import ColumnManager

class TableWrapper():
    def __init__(self, tableID, columns):
        self.table_id = tableID
        self.columns = columns

        self.column_to_mem = {}
        for column in columns:
            self.column_to_mem[column] = ColumnManager.ColumnManager(tableID,column)


    def get(self, column, key):
        return self.column_to_mem[column].get(key)

    def put(self, key, column, value):
        self.column_to_mem[column].put(key, value)

    def getlist(self, column):
        self.column_to_mem[column].getinit()
        result=self.column_to_mem[column].getlist()
        while result!=None:
            yield result
            result=self.column_to_mem[column].getlist()

    def remove(self, key):
        for manager in self.column_to_mem.values():
            manager.remove(key)












