#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 17:32:06 2017

@author: root
"""
import TableWrapper

class LSMDatabase():
    def __init__(self):
        self.memorytable={}
        self.valuestable={}
        self.idnotused=0
    def create(self,valuesName):
        if type(valuesName)!='set':
            return -1
        result=self.idnotused
        self.idnotused+=1
        self.valuestable[result]=valuesName
        self.memorytable[result]=TableWrapper(result,list(valuesName))
        return result
    def set(self,tableID,keyvalue,setList):
        try:
            valuestable=self.valuestable[tableID]
        except:
            return -1
        for i in setList:
            if i not in valuestable:
                return -2
        wrapper=self.memorytable[tableID]
        for i in setList:
            wrapper.put(keyvalue,i,setList[i])
        return 0


    def delete(self,tableID,keyvalue):
        try:
            wrapper=self.memorytable[tableID]
        except:
            return -1
        wrapper.remove(keyvalue)
    def select(self,tableID,valuesName,whereFunction=None):
        try:
            oldvaluesName=self.valuestable[tableID]
        except:
            return -1
        for column in valuesName:
            if column not in oldvaluesName:
                return -2
        newid=self.create(valuesName)
        wrapper=self.memorytable[tableID]
        if whereFuntion==None:
            for column in valuesName:
                tmp=wrapper.get(column)
                for row in tmp:
                    self.set(newid,row[0],[(column,row[1])])
        else:
            for column in valuesName:
                tmp=wrapper.get(column)
                for row in tmp:
                    if whereFunction[i[0]]:
                        self.set(newid,row[0],[(column,row[1])])
        return newid
