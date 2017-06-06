#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 20:39:23 2017

@author: root
"""

def wherefunction(s):
    return s=='5'

import database
testDB = database.LSMDatabase()

T1=testDB.create(set(['name','birthplace']))
T2=testDB.create(set(['birthyear','birthmonth','bitrhday']))
for i in range(128):
    testDB.append(T1,str(1+i),dict({'name': ("abc"+str(i)),'birthplace':("192.168.0."+str(i))}))
    testDB.append(T2,str(3+i),dict({'birthyear':str(1993+i//10),'birthmonth':str(1+i%12),'bitrhday':str(1+i%28)}))
testDB.select(T1,wherefunction)
