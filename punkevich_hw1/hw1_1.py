#!/usr/bin/python3
 
import os
import csv
from bs4 import BeautifulSoup

inputdir = './input_ameritrade/'
directory = os.fsencode(inputdir)
headers = ['id','name','ticker','cap','avgvolume','eps','price']

with open('hw1_1_output.csv','w') as csvfile: # clear output file and add headers
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
                        dldata = soup.find_all('dl')
                        for dlitem in dldata:        
                                if (dlitem.find('dt').text == 'Closing Price'):
                                        price = dlitem.find('dd').text[1:]
                                elif (dlitem.find('dt').text == 'Market Cap'):
                                        cap = dlitem.find('dd').text
                                elif (dlitem.find('dt').text == '10-day average volume:'):
                                        avgvolume = dlitem.find('dd').text
                        ticker = soup.find('input', {'id': 'symbol-lookup'}).get('value')
                        name = soup.find('span', class_='stock-title').get_text()
                        
                        with open('hw1_1_output.csv', 'a') as csvfile:
                                writer = csv.DictWriter(csvfile, fieldnames=headers)
                                writer.writerow({'id': id_, 'name': name, 'price': price, 'ticker': ticker, 'cap': cap})
                                id_ += 1
                                     
