#!/usr/bin/env python3

from pyjarowinkler import distance
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
from itertools import product
import csv
import re
import hashlib

WORD_RE = re.compile(r"[\w']+")

class MRDataFusion(MRJob):

        def mapper1(self, _, line):
                data = line[1:len(line)-3].split(',')
                dublicate_id = data[7]
                docname = jobconf_from_env('map.input.file')
                #yield 'ALL', (dublicate_id, data[:7])   
                #yield 'ALL', (docname,line)
                yield 'ALL', (docname, data)
        
        def comparer(self, str1, str2):
                if (str1 == str2): 
                        return str1
                        #return str1+' * '+str2
                elif (str1 > str2):
                        return str1
                        #return '====='
                else:
                        return str2
        
        def reducer1(self, key, values):
                for (docname1, value1), (docname2, value2) in product(values, repeat = 2):
                        #data1 = value1.split(',')
                        #data2 = value2.split(',')
                        if (value1[7] == value2[7]) and (value1 != value2) and (docname1 == 'file://input/part-00000'):
                                result = []
                                for i in range(len(value1) - 1):
                                        result.append(self.comparer(value1[i], value2[i]))
                                yield result, docname1
                                
        
        def mapper2(self, key, metrics):
                obj1, obj2 = key[2], key[3]
                ticker1, ticker2 = obj1.split(',')[2], obj2.split(',')[2]
                key.append(metrics)
                yield (key), (ticker1, ticker2)
        
        def reducer2(self, key, values):
                arr = list(values)
                for ticker1, ticker2 in arr:
                        jaro_dist = distance.get_jaro_distance(ticker1, ticker2, winkler=True, scaling=0.1)
                        yield (key), jaro_dist 
        
        def mapper3(self, key, metrics):
                obj1, obj2 = key[2], key[3]
                price1, price2 = obj1.split(',')[6], obj2.split(',')[6]
                key.append(metrics)
                yield (key), (price1, price2)
        
        def reducer3(self, key, values):
                data = list(values)
                for price1, price2 in data:
                        price_comparison = round(1 - abs(float(price1) - float(price2)))
                        yield (key), price_comparison
                        
        def mapper4(self, key, price_comparison):
                jd_name, jd_ticker = key[4], key[5]
                yield (key[:4]), (jd_name, jd_ticker, price_comparison)
                
        def reducer4(self, key, metrics):
                data = list(metrics)
                for jd_name, jd_ticker, price_comp in data:
                        result = ((float(jd_name) + float(jd_name)) / 2) * jd_name
                if (result > 0.75 and result < 1.00):
                        ticker = key[2].split(',')[2]
                        dublicate_id = hashlib.md5(ticker.encode()).hexdigest()
                        yield (key[2] + ',' + dublicate_id), 1               
        
        def steps(self):
        	return [
            		MRStep(
            			mapper=self.mapper1,
            			reducer=self.reducer1
            			)
            		#MRStep(
            		#	mapper=self.mapper2,
            		#	reducer=self.reducer2,
            		#	),
            		#MRStep(
		#		mapper=self.mapper3,
		#		reducer=self.reducer3,
		#		),
		#	MRStep(
		#		mapper=self.mapper4,
		#		reducer=self.reducer4,
		#		),
        	]


if __name__ == '__main__':
    MRDataFusion.run()
    
        
