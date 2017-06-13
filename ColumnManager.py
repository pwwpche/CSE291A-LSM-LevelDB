# -*- coding: utf-8 -*-

import SSTable

class ColumnManager:

    def __init__(self, tableID,column_name):
        self.filename = str(tableID)+'_'+column_name
        self.mem = {}
        self.imTable = []
        self.maxLength = 8
        self.ssTable = SSTable.SSTable(self.filename)
        self.now = -1
        self.fileNumber = self.ssTable.getnumber()
        self.imTableNumber = len(self.imTable) + self.fileNumber

    def getinit(self):
        self.now = -1
        self.fileNumber = self.ssTable.getnumber()
        self.imTableNumber = len(self.imTable) + self.fileNumber
    def get(self, key):
        if key in self.mem:
            return self.mem[key]
        for i in range(len(self.imTable)):
            if key in self.imTable[i]:
                return self.imTable[i][key]
        return self.ssTable.get(key)

    def put(self, key, value):
        self.mem[key] = value

        if len(self.mem) > self.maxLength:
            self.imTable.append(self.mem)
            self.mem = {}

        if len(self.imTable) > self.maxLength:
            totalDict = dict()
            for i in range(len(self.imTable)):
                for key in self.imTable[i]:
                    totalDict[key] = self.imTable[i][key]
            self.ssTable.store(totalDict)
            self.imTable = []

    def remove(self, key):
        if key in self.mem:
            del self.mem[key]
        else:
            self.put(key,None)

    def getlist(self):
        self.now += 1
        print self.now,self.fileNumber
        if self.now < self.fileNumber:
            return self.ssTable.getfile()
        elif self.now < self.imTableNumber:
            return self.imTable[self.now - self.fileNumber]
        elif self.now == self.imTableNumber:
            return self.mem
        else:
            return None
            