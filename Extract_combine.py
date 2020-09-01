# -*- coding: utf-8 -*-
"""
Created on Tue Aug 25 18:34:22 2020

@author: sahil.r
"""
import Plot_AQI
#from Plot_AQI import avg_data_2013
import requests
import sys 
import pandas as pd
import os
from bs4 import BeautifulSoup
import csv

def met_data(month, year):
    file_html = open(r'C:\Users\sahil.r\AQI\Data\Html_Data\{}\{}.html'.format(year, month), 'rb')
    plain_text=file_html.read()
    
    tempD=[]
    finalD=[]
    
    soup = BeautifulSoup(plain_text, "lxml")
    soup.findAll('table', {'class': 'medias mensuales numspan'})
    for table in soup.findAll('table', {'class': 'medias mensuales numspan'}):
        for tbody in table:
            for tr in tbody:
                a =tr.get_text()
                tempD.append(a)
                
    rows = len(tempD)/15
    
    for time in range(round(rows)):
        newtempD=[]
        for i in range(15):
            newtempD.append(tempD[0])
            tempD.pop(0)
        finalD.append(newtempD)
        
    length = len(finalD)    #Find length to use it to drop the last row
    
    finalD.pop(length-1)    #To drop the last value as it has no proper meaning
    finalD.pop(0)           #Drop column names we will add it later manually
    
    for a in range(len(finalD)):
        finalD[a].pop(6)
        finalD[a].pop(13)
        finalD[a].pop(12)
        finalD[a].pop(11)
        finalD[a].pop(10)
        finalD[a].pop(9)
        finalD[a].pop(0)             #Popping columns that dont have any values
    
    return finalD
    
def data_combine(year, cs):         #To combine all the years data in one file
    for a in pd.read_csv(r'C:\Users\sahil.r\AQI\Data\Real_Data\real_' + str(year) + '.csv', chunksize = cs):
        df = pd.DataFrame(data = a)
        mylist=df.values.tolist()
    return mylist



if __name__ == "__main__":
    if not os.path.exists("Data/Real_Data"):      #If folder does not create create folder
         os.makedirs("Data/Real_Data")
    for year in range(2013,2017):     
         final_data=[]
         with open('Data/Real_Data/real_' +str(year)+ '.csv','w') as csvfile:     #Create CSV file
             wr = csv.writer(csvfile, dialect = "excel")                        #Styling in Excel format
             wr.writerow(['T', 'TM', 'Tm', 'SLP', 'H', 'VV', 'V', 'VM', 'PM 2.5'])  
         for month in range(1,13):
            temp=met_data(month, year)
            final_data=final_data+temp
            
         pm = getattr(sys.modules[__name__], 'avg_data_{}'.format(year))()
        
         if len(pm)==364:
            pm.insert(364,'-')
            
         for i in range(len(final_data) - 1):
            final_data[i].insert(8,pm[i])
        
         with open('Data/Real_Data/real_' +str(year)+ '.csv','a') as csvfile: 
            wr = csv.writer(csvfile, dialect = "excel")
            for row in final_data:
                flag = 0
                for elem in row:
                    if elem =="" or elem =="-":      #Some elements in the file are empty elements 
                        flag = 1                     #so to remove them we have written this code
                
                if flag!=1:
                    wr.writerow(row)
        
    data_2013=data_combine(2013,600)        #You take chunksize if your ram is less so you take only a chunk
    data_2014=data_combine(2014,600)        #of data and you can take any values
    data_2015=data_combine(2015,600)        
    data_2016=data_combine(2016,600)     
    
    total=data_2013+data_2014+data_2015+data_2016
    with open('Data/Real_Data/Real_Combine.csv','w') as csvfile:     #Create CSV file
             wr = csv.writer(csvfile, dialect = "excel")                        #Styling in Excel format
             wr.writerow(['T', 'TM', 'Tm', 'SLP', 'H', 'VV', 'V', 'VM', 'PM 2.5'])
             wr.writerows(total)
             
df = pd.read_csv('Data/Real_Data/Real_Combine.csv')