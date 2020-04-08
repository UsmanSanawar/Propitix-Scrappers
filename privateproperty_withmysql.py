from requests import Session
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
from config import *
from genaric import *
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
import re
import requests



def scrapAll(csvFileName,s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')

	cols = ['Title','Address','Price','Beds','Bath','Area','URL','DateScrapped']
	createFile(csvFileName,cols)

	
	# categories = ['short-let','property-for-sale','property-for-rent']
	categories = ['property-for-sale','property-for-rent']


	for category in categories:
	    
	    url = 'https://www.privateproperty.com.ng/' + category

	    while(True):
	    # for i in range(3):
	        site  = s.get(url)
	        soup = BeautifulSoup(site.text,'html.parser')
	        maindiv = soup.find('div',{'class':'result-card-container'})
	        houses = maindiv.find_all('div',{'class':'result-card-item'})
	        for house in houses:
	        	try:
		            sub_site = s.get('https://www.privateproperty.com.ng' + house.a['href'])
		            sub_soup = BeautifulSoup(sub_site.text,'html.parser')
		            house_detail = sub_soup.find('div',{'class':'result-card-item'})
		            price = house_detail.find('a',{'class':'item-price'}).text.strip().replace(',','').replace('N','').replace(' per year','')
		            title = house_detail.find('span',{'span','property-title h3'}).text.strip()
		            address = house_detail.find('div',{'class':'property-location'}).text.strip()
		            area = house_detail.find('span',{'class':'h-area'})
		            if area:
		                area = area.text.strip()
		            else:
		                area = 0

		            beds = house_detail.find('span',{'class':'h-beds'})
		            if beds:
		                beds = beds.text.strip()
		            else:
		                beds = 0

		            baths = house_detail.find('span',{'class':'h-baths'})
		            if baths:
		                baths = baths.text.strip()
		            else:
		                baths = 0
		            url = 'https://www.privateproperty.com.ng' + house.a['href']
		            csvRow = [title,address,price,beds,baths,area,url,formatted_date]
		            writeInCSV(csvFileName,csvRow)
		        except:
		        	continue

	        np = soup.find('a',{'aria-label':'Next'})
	        if np:
	            np = np['href']
	            print(np)
	            if np == 'javascript:void(0)':
	                break
	            else:
	                url = np




def checkAndUpdate(dbconn,dbname, tablename, s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
	categories = ['property-for-sale','property-for-rent']	

	for category in categories:
	    
	    url = 'https://www.privateproperty.com.ng/' + category

	    while(True):
	    # for i in range(3):
	        site  = s.get(url)
	        soup = BeautifulSoup(site.text,'html.parser')
	        maindiv = soup.find('div',{'class':'result-card-container'})
	        houses = maindiv.find_all('div',{'class':'result-card-item'})
	        for house in houses:
	        	try:
		            sub_site = s.get('https://www.privateproperty.com.ng' + house.a['href'])
		            sub_soup = BeautifulSoup(sub_site.text,'html.parser')
		            house_detail = sub_soup.find('div',{'class':'result-card-item'})
		            price = house_detail.find('a',{'class':'item-price'}).text.strip().replace(',','').replace('N','').replace(' per year','')
		            title = house_detail.find('span',{'span','property-title h3'}).text.strip()
		            address = house_detail.find('div',{'class':'property-location'}).text.strip()
		            area = house_detail.find('span',{'class':'h-area'})
		            if area:
		                area = area.text.strip()
		            else:
		                area = 0

		            beds = house_detail.find('span',{'class':'h-beds'})
		            if beds:
		                beds = beds.text.strip()
		            else:
		                beds = 0

		            baths = house_detail.find('span',{'class':'h-baths'})
		            if baths:
		                baths = baths.text.strip()
		            else:
		                baths = 0
		            url = 'https://www.privateproperty.com.ng' + house.a['href']
		            csvRow = [title,address,price,beds,baths,area,url,formatted_date]
		            mySql_insert_query = """INSERT INTO Privateproperty ( Title, Address, Price, Beds, Bath, Area, URL, DateScrapped ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
		            checkUrlInDB(url,csvRow,tablename,dbconn,mySql_insert_query)
		        except:
		        	continue

	        np = soup.find('a',{'aria-label':'Next'})
	        if np:
	            np = np['href']
	            print(np)
	            if np == 'javascript:void(0)':
	                break
	            else:
	                url = np


if __name__ == '__main__':
    s= Session()
    csvFileName = csv_path + 'privateproperty.csv'
    tablename = 'Privateproperty'
    changes_tablename = 'Privateproperty_Changes'
    dbname = 'property'
    connection = mysql.connector.connect(host='localhost',
                                         database='property',
                                         user=dbusername,
                                         password=dbpassword)
    mySql_Create_Table_Query = """CREATE TABLE {} ( 
         Id int NOT NULL AUTO_INCREMENT,
         Title varchar(255) NOT NULL,
         Address varchar(255) NOT NULL,
         Price bigint NOT NULL,
         Beds Varchar(255) ,
         Bath varchar(255),
         Area varchar(255),
         URL varchar(255) NOT NULL,
         DateScrapped Date NOT NULL,
         PRIMARY KEY (Id)) """
    checkTable = checkTableExists(connection,tablename)
    if checkTable == False:
        print("No Table Found With Name {}".format(tablename))
        createTable(connection,mySql_Create_Table_Query,tablename)
        scrapAll(csvFileName,s)
        print("All Data is Scrapped and saved in: {}".format(csvFileName))

        data = pd.read_csv(csvFileName)
        addDataToDB(data,dbname,tablename)
        print("Data Populated in Table: {}".format(tablename))

    else:
    	print("Table already Exist.\nChecking to updated results...")
    	checkAndUpdate(connection, dbname, tablename, s)





