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
from time import clock
def wherefunction(tup):
    return tup=="a5"
import LSMDatabase
database=LSMDatabase.LSMDatabase()

start_time=clock()
t1=database.create(set(["name"]))
end_time=clock()
print "create ",t1, " time ",(end_time-start_time)

start_time=clock()
t2=database.create(set(["birthday","birthplace"]))
end_time=clock()
print "create ",t2, " time ",(end_time-start_time)

print t1,t2

start_time=clock()
for i in range(128):
    tmp=database.append(t1,"a"+str(i),{"name":"a"*(i/6+1)})
    tmp=database.append(t2,"a"+str(i),{"birthday":"b"*(i/6+1),"birthplace":"c"*(i/6+1)})
end_time=clock()
print "append data time ",(end_time-start_time)

start_time=clock()
t3=database.select(t1)
end_time=clock()
print "select ",t1, " time ",(end_time-start_time)
print "select finished"

start_time=clock()
t4=database.join(t1,t2)
end_time=clock()
print "join ",t1,t2 ," time ",(end_time-start_time)
print "join finished"

print t3, "table show:"
database.show(t3)

print t4, "table show:"
database.show(t4)