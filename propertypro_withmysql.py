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
	cols = ['Title','Address','Price','Beds','Bath','Toilets','DateAdded','URL','DateScrapped']

	createFile(csvFileName,cols)


	site  = s.get('https://www.propertypro.ng/property-for-sale')

	while(True):
	# for i in range(30):
	    soup = BeautifulSoup(site.text,'html.parser')
	    products = soup.find_all('div',{"class":"property-bg"})
	    for product in products:
	        url = product.a['href']
	        title = product.h2.text.strip()
	        address = product.h3.text.strip()
	        price = product.find('p',{'class':'prop-price'}).text.strip()
	        price = price.replace('₦ ','')
	        price  = price.replace(chr(160),"").replace(',','')
	        price = int(re.search(r'\d+', price).group())

	        prop =product.find('div',{'class':'prop-features'})
	        spanall= prop.find_all('span')
	        bed = spanall[1].text
	        bath = spanall[2].text
	        toilet = spanall[3].text

	        date = product.find('span',{'class':'prop-date'}).text.replace("Added ","")
	        csvRow = [title, address, price, bed, bath, toilet, date, url,formatted_date]
	        writeInCSV(csvFileName,csvRow)
	    next_page = soup.find('ul',{'class','pagination'})
	    next_page = next_page.find('a',{'alt':'view next property page'})
	    if next_page:
	    	next_page = next_page['href']
	    	url = 'https://www.propertypro.ng' + next_page
	    	site  = s.get(url)
	    	print(url)
	    else:
        	break
#


def checkAndUpdate(dbconn,dbname, tablename, s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')	

	site  = s.get('https://www.propertypro.ng/property-for-sale')

	while(True):
	# for i in range(30):
	    soup = BeautifulSoup(site.text,'html.parser')
	    products = soup.find_all('div',{"class":"property-bg"})
	    for product in products:
	        url = product.a['href']
	        title = product.h2.text.strip()
	        address = product.h3.text.strip()
	        price = product.find('p',{'class':'prop-price'}).text.strip()
	        price = price.replace('₦ ','')
	        price  = price.replace(chr(160),"").replace(',','')
	        price = int(re.search(r'\d+', price).group())

	        prop =product.find('div',{'class':'prop-features'})
	        spanall= prop.find_all('span')
	        bed = spanall[1].text
	        bath = spanall[2].text
	        toilet = spanall[3].text

	        date = product.find('span',{'class':'prop-date'}).text.replace("Added ","")
	        csvRow = [title, address, price, bed, bath, toilet, date, url,formatted_date]

	        mySql_insert_query = """INSERT INTO Propertypro ( Title, Address, Price, Beds, Bath, Toilets, DateAdded, URL, DateScrapped ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
	        checkUrlInDB(url,csvRow,tablename,dbconn,mySql_insert_query)

	    next_page = soup.find('ul',{'class','pagination'})
	    next_page = next_page.find('a',{'alt':'view next property page'})
	    if next_page:
	    	next_page = next_page['href']
	    	url = 'https://www.propertypro.ng' + next_page
	    	site  = s.get(url)
	    	print(url)
			# next_page = next_page.find('a',{'alt':'view next property page'})['href']
	    else:
        	break
#


if __name__ == '__main__':
    s= Session()
    csvFileName = csv_path + 'propertypro.csv'
    tablename = 'Propertypro'
    changes_tablename = 'Propertypro_Changes'
    CheckForDB()
    connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbusername,
                                         password=dbpassword)
    mySql_Create_Table_Query = """CREATE TABLE {} ( 
         Id int NOT NULL AUTO_INCREMENT,
         Title text NOT NULL,
         Address text NOT NULL,
         Price bigint NOT NULL,
         Beds Varchar(255) ,
         Bath varchar(255),
         Toilets varchar(255),
         DateAdded varchar(255),
         URL text NOT NULL,
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
        