#!/usr/bin/python3
 
import os
import csv
import re
from bs4 import BeautifulSoup

inputdir = './input_yahoo/'
directory = os.fsencode(inputdir)
headers = ['id','name','ticker','cap','avgvolume','eps','price']

with open('hw1_2_output.csv','w') as csvfile: # clear output file and add headers
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
csvfile.close()
id_ = 0
for file in os.listdir(directory):
        filename = os.fsdecode(file)

        if filename.endswith(".html"):
                with open(inputdir+filename, "r") as f:
                        contents = f.read()
                        soup = BeautifulSoup(contents, 'lxml')
                        name = soup.find('h1', {'data-reactid': '7'}).get_text().replace(',','')
                        ticker = name.split('(', 1)[1].split(')')[0]
                        price = soup.find('span', {'class':'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)', 'data-reactid': '31'}).text
                        eps = soup.find('span', {'data-reactid': '99'}).get_text()
                        avgvolume = soup.find('span', {'data-reactid': '76'}).get_text().replace(',','.')
                        with open('hw1_2_output.csv', 'a') as csvfile:
                                writer = csv.DictWriter(csvfile, fieldnames=headers)
                                writer.writerow({'id': id_, 'name': name, 'price': price, 'ticker': ticker, 'eps': eps, 'avgvolume': avgvolume})
                                id_ += 1
                                     
