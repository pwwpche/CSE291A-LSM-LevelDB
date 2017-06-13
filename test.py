#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 20:39:23 2017

@author: root
"""
'''
from SSTable import *

def test_SSTable():
    sst = SSTable("ss1")


    # for i in range(10):
    #     mem = {}
    #     for j in range(100):
    #         mem[i * j] = i
    #     #mem = {1:1, 2:2, 3:3}
    #     sst.store(mem)

    sst.getnumber()
    while True:
        data = sst.getfile(0)
        if data is None:
            break
        print data

    print sst.get(666)

test_SSTable()
'''
import LSMDatabase
database=LSMDatabase.LSMDatabase()
t1=database.create(set(["name"]))
t2=database.create(set(["birthday","birthplace"]))
print t1,t2
for i in range(128):
    tmp=database.append(t1,"a"+str(i),{"name":"a"*(i/6+1)})
    #print(tmp)
    tmp=database.append(t2,"a"+str(i),{"birthday":"b"*(i/6+1),"birthplace":"c"*(i/6+1)})
    #print(tmp)