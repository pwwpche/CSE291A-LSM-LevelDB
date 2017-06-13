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
        if type(valuesName)!=set:
            return -1
        result=self.idnotused
        self.idnotused+=1
        self.valuestable[result]=valuesName
        self.memorytable[result]=TableWrapper.TableWrapper(result,list(valuesName))
        return result
    def append(self,tableID,keyvalue,setList):
        try:
            valuestable=self.valuestable[tableID]
        except:
            return -1
        for i in setList:
            if i not in valuestable:
                return -2
        wrapper=self.memorytable[tableID]
        setdict={}
        for i in valuestable:
            setdict[i]=""
        for i in setList:
            setdict[i]=setList[i]
        for i in setdict:
            wrapper.put(keyvalue,i,setdict[i])
        return 0
    def setvalue(self,tableID,keyvalue,setList):
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
        return 0
    def select(self,tableID,valuesName=None,whereFunction=None):
        try:
            oldvaluesName=self.valuestable[tableID]
        except:
            return -1
        if valuesName==None:
            valuesName=oldvaluesName
        for column in valuesName:
            if column not in oldvaluesName:
                return -2
        newid=self.create(valuesName)
        wrapper=self.memorytable[tableID]
        if whereFunction==None:
            for column in valuesName:
                for tmp in wrapper.getlist(column):
                    #print "check"
                    for key in tmp:
                        self.setvalue(newid,key,{column:tmp[key]})
        else:
            for column in valuesName:
                for tmp in wrapper.getlist(column):
                    for key in tmp:
                        if whereFunction[key]:
                            self.setvalue(newid,key,{column:tmp[key]})
        return newid
    def show(self,tableID):
        result=dict()
        try:
            values=list(self.valuestable[tableID])
        except:
            return -1
        if not values:
            return -2
        value=values[0]
        wrapper=self.memorytable[tableID]
        for tmp in wrapper.getlist(value):
            for key in tmp:
                if key not in result:
                    result[key]={}
                result[key][value]=tmp[key]
                if tmp[key]==None:
                    del result[key]
        for value in values[1:]:
            for i in result:
                result[i][value]=wrapper.get(value,i)
        print(result)
        return 0
