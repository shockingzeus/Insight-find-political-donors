#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 28 17:29:05 2017

@author: Xiaoqing
"""

import sys
import time
import bisect
# We will create two python dictionaries for the data. The keys take the form 
# of tuple ("CMTE_ID", "ZIP_CODE") for the first dict, and ("CMTE_ID", "DATE") for the second.
# The value fo the dict would be a sorted list of "TRANSACTION_AMT".



class datamanager_byzip():
# This create a python dictionary and manage the streaming data    
    
    def __init__(self, out_byzip):
        self.mydict = {}
        self.outf = out_byzip
        f=open(out_byzip, 'w')
        f.close()
        
    def update(self, newdata):
        key = (newdata[0], newdata[10][:5])
        money = round(float(newdata[14]))
        if key in self.mydict.keys():
            bisect.insort(self.mydict[key], money)
        else:
            self.mydict[key] = [money]
        info="|".join([key[0], key[1][:5], str(sortedmedian(self.mydict[key])),
                       str(len(self.mydict[key])), str(sum(self.mydict[key]))])+'\n' 
        with open(self.outf, 'a') as f:
            f.write(info)
         
    def __exit__(self):
        self.outf.close()
    
class datamanager_bydate():
# This create a python dictionary and manage the data as a whole.  
# As python dictionary is intrinsically unordered, we keep a sorted list of keys (tuple).
# Note that key[1], i.e. the date needs to be reformated as YYYYMMDD for sorting.
    
    def __init__(self, out_bydate):
        self.mydict = {}
        self.outf = out_bydate
        
    def process(self, alldata):
# read all the data in RAM to sort all the keys in dictionary in one go.
# If the file is too big, use the streaming version.  
        for line in alldata:
            data = line.rstrip('\n').rsplit('|')
            if isvalid(data):
                if isdate(data[13]):
                    key = (data[0],self.YMD(data[13]))
                    money = round(float(data[14]))
                    if key in self.mydict.keys():
                        bisect.insort(self.mydict[key],money)
                    else:
                        self.mydict[key]=[money]
        sorted_keys = sorted(self.mydict.keys())
        with open(self.outf, 'w') as f:
            for key in sorted_keys:
                info="|".join([key[0], str(key[1])[4:]+str(key[1])[:4], str(sortedmedian(self.mydict[key])),
                               str(len(self.mydict[key])), str(sum(self.mydict[key]))])+'\n' 
                f.write(info)
        
# Old code that deal with data stream. Update everything on the fly, probably slower.
        
#    def update(self, newdata):
#        newkey = (newdata[0], self.Re_date(newdata[13]))
#        money = round(float(newdata[14]))
#        if newkey in self.mydict.keys():
#            bisect.insort(self.mydict[newkey][0], money)
#            info="|".join([newdata[0], newdata[13], str(sortedmedian(self.mydict[newkey][0])),
#                           str(len(self.mydict[newkey][0])), str(sum(self.mydict[newkey][0]))])+'\n'
#            self.mydict[newkey][1]=info
#       else:
#            bisect.insort(self.mykeys, newkey)
#            self.mydict[newkey] = [[money],]
#            info="|".join([newdata[0], newdata[10], str(money),
#                           "1",str(money)])+'\n'
#            self.mydict[newkey].append(info)
#        with open(out_bydate,'w') as f:
#            for key in self.mykeys:
#                f.write(self.mydict[key][1])
        
    @staticmethod
    def YMD(date):
# Reformate the date as an integer so it can be easily sorted. 
        return int(date[4:]+date[:2]+date[2:4])
        
    def __exit__(self):
        self.f.close()

def isvalid(datalst):
# check whether the incoming data is valid: first it should have 21 positions, 
# then it should have "other_ID" (position 15) empty and has "CMTE_ID" (position 0) 
# and "TRANSACTION_AMT (position 14)" is a valid number.     
    if len(datalst)!=21:
        return False
    elif datalst[15]!="" or datalst[0] =="":
        return False
    else:
        try:
            float(datalst[14])
            return True
        except:
            return False
        
def iszip(zipcode):
#check if zipcode is valid. Here we simply check whether the first 5 digits is a integer.
#We could do web crawing and see if the zipcode really exist - probably overkill here.
    if len(zipcode)>=5:
        try:
            int(zipcode[:5])
            return True
        except:
            return False
    else:
        return False
   
def isdate(date):
#check if the date is valid using the time module
    if len(date) == 8:
        try:
            time.strptime(date,"%m%d%Y")
            return True
        except:
            return False
    else:
        return False

def sortedmedian(array):
# return the median of a sorted array.
    l = len(array)
    return array[l//2] if l%2 ==1 else round((array[l//2-1]+array[l//2])/2)   

def main(inputf, out_byzip, out_bydate):
    with open(inputf, "r") as data:
        lines = [line.rstrip('\n') for line in data]  
   
    DM_byzip = datamanager_byzip(out_byzip)
    DM_bydate = datamanager_bydate(out_bydate)  
  
    DM_bydate.process(lines)
# Wholesale action
        
    for line in lines:
        stream =  line.rsplit("|")
# Simulated data stream.
        if isvalid(stream):                
            if iszip(stream[10]):
                DM_byzip.update(stream)


if __name__=="__main__":
    main(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]))