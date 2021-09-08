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

fieldnames = ['id','name','sector','price','volume','ticker','ebitda','net profit','capitalization']
fields = 'id,name,ticker,cap,avgvolume,eps,price'
path = '/home/roman/Desktop/hw2/hw1_1_output.csv'


class MREntityResolution(MRJob):

        def mapper1(self, _, line):
                docname = jobconf_from_env('map.input.file')
                yield 'ALL', (docname, line)   

        def reducer1(self, key, values):
                for (docname1, value1), (docname2, value2) in product(values, repeat = 2):
                        if (value1 != value2) and docname1 != docname2:
                                str1 = value1.split(',')
                                str2 = value2.split(',')
                                name1, name2 = str1[1], str2[1]
                                yield (docname1, docname2, value1, value2), distance.get_jaro_distance(name1, name2, winkler=True, scaling=0.1)

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
                arr = list(values)
                for price1, price2 in arr:
                        price_comparison = round(1 - abs(float(price1) - float(price2)))
                        yield (key), price_comparison
                        
        def mapper4(self, key, price_comparison):
                jd_name, jd_ticker = key[4], key[5]
                yield (key[:4]), (jd_name, jd_ticker, price_comparison)
                
        def reducer4(self, key, metrics):
                arr = list(metrics)
                for jd_name, jd_ticker, price_comp in arr:
                        result = ((float(jd_name) + float(jd_name)) / 2) * jd_name
                if (result > 0.75 and result < 1.00):
                        
                yield key, result               
        
        def steps(self):
        	return [
            		MRStep(
            			mapper=self.mapper1,
            			reducer=self.reducer1
            			),
            		MRStep(
            			mapper=self.mapper2,
            			reducer=self.reducer2,
            			),
            		MRStep(
				mapper=self.mapper3,
				reducer=self.reducer3,
				),
			MRStep(
				mapper=self.mapper4,
				reducer=self.reducer4,
				),
        	]


if __name__ == '__main__':
    MREntityResolution.run()
        
