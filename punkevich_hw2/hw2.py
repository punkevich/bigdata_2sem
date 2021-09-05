#!/usr/bin/env python3

from pyjarowinkler import distance
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
from itertools import product
import csv
import re

WORD_RE = re.compile(r"[\w']+")

#print(distance.get_jaro_distance("Юнипро, акция обыкновенная (RU000A0JNGA5, UPRO)", "Мосэнерго", winkler=True, scaling=0.1))

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
                                yield (docname1, docname2), (value1, value2, distance.get_jaro_distance(name1, name2, winkler=True, scaling=0.1))

        def mapper2(self, key, values):
                ticker1 = values[0].split(',')[2]
                ticker2 = values[1].split(',')[2]
                yield (key, (values, ticker1, ticker2)) 
        
        def reducer2(self, key, values):
                #ticker1 = values[1].split()[0]
                #ticker2 = values[2].split()[0]
                #yield (key, (values, distance.get_jaro_distance(ticker1, ticker2, winkler=True, scaling=0.1)))
                yield type(values),1 
        
        def steps(self):
        	return [
            		MRStep(
            			mapper=self.mapper1,
            			reducer=self.reducer1
            			),
            		MRStep(
            			mapper=self.mapper2,
            			reducer=self.reducer2,
            			)
            		#MRStep(
			#	mapper=self.get_termscount,
			#	reducer=self.get_tfidf,
			#	),
			#MRStep(
			#	combiner=self.filter,
			#	reducer=self.list_of_doc,
			#	),
        	]


if __name__ == '__main__':
    MREntityResolution.run()
        
