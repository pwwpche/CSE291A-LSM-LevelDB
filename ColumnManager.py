# -*- coding: utf-8 -*-

import SSTable
class ColumnManager():
    def __init__(self, tableID,column_name):
        self.filename=str(tableID)+'_'+column_name
        self.mem = {}
        self.imTable = []
        self.maxLength=8
        self.ssTable=SSTable()
    def get(self, key):
        if key in self.mem:
            return self.mem[key]
        for i in range(reversed(len(self.imTable))):
            if key in self.imTable[i]:
                return self.imTable[i][key]
        return self.ssTable.searchDisk(key)

    def put(self, key, value):
        self.mem[key]=value
        if len(self.mem)>self.maxLength:
            self.imTable.append(self.mem)
            self.mem={}
        if len(self.imTable)>self.maxLength:
            file=dict()
            for i in range(reversed(len(self.imTable))):
                for key in self.imTable[i]:
                    if key not in file:
                        file[key]=self.imTable[i][key]
            self.ssTable.store(file)
            self.imTable=[]
    def remove(self, key):
        self.mem[key]=None
    def getInit(self):
        self.now=-1
        self.filenumber=self.ssTable.getnumber()
        self.imtablenumber=len(self.imTable)+self.filenumber
    def getlist(self):
        self.now+=1
        if self.now<self.filenumber:
            return self.ssTable.getfile(self.now)
        elif self.now<self.imtablenumber:
            return self.imtable[self.now-self.filenumber]
        elif self.now==self.imtablenumber:
            return self.mem
        else:
            return None
            