from requests import Session
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
import csv
from config import *
from genaric import *
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
import re


def scrapAll(csvFileName,s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
	cols = ['Title','Address','Price','Beds','Bath','Area','URL','DateScrapped']
	createFile(csvFileName,cols)

	categories = ['property-for-sale','property-to-rent']

	for category in categories:
	    site  = s.get('https://www.property24.com.ng/'+category+'?Page=1')
	    soup = BeautifulSoup(site.text,'html.parser')
	    while(True):
	    # for i in range(2):
	        soup = BeautifulSoup(site.text,'html.parser')
	        houses = soup.find_all('div',{'class':'propertyTileWrapper'})
	        for house in houses:
	            title =house.a['title']
	            link = 'https://www.property24.com.ng'+house.a['href']
	            price = house.span
	            if price:
	                price  = price.text.strip()
	                price = price.replace('₦ ','')
	                price  = price.replace(chr(160),"")

	                price = int(re.search(r'\d+', price).group())
	                # price  = price.replace(chr(160),"")
	                # price = price.replace('₦ ','')
	                if price == "":
	                    price = 0
	            else:
	                price = 0
	            address = house.find('div',{'class','left address'})
	            if address:
	                address = address.text
	                if address == "":
	                    address = "No Address Found"
	#                 else:
	#                     address = "No Address Found"
	            specs =house.find('div',{'class','left detailsBorder detailsContainer'})
	            if specs:
	                bed_bath = specs.find_all('span')
	                if house.find('img',{'src':'/Content/Images/BedTile.png?58c88b '}):
	                    bed =  bed_bath[0].text
	                    if house.find('img',{'src':'/Content/Images/BathTile.png?498121'}):
	                        bath =  bed_bath[1].text
	                else:
	                    bed =0
	                    if house.find('img',{'src':'/Content/Images/BathTile.png?498121'}):
	                        bath =  bed_bath[0].text
	                    else:
	                        bath=0

	                size = specs.select_one("span[class='sizeUnits']")
	                if size:
	                    size = size.text.strip().replace(chr(160),"")
	                else:
	                    size = 0
	            else:
	                bed =0
	                bath =0
	                size=0
	            csvRow = [title,address,price,bed,bath,size,link,formatted_date]
	            with open(csvFileName,"a",newline="") as fp:
	                wr = csv.writer(fp, dialect='excel')
	                wr.writerow(csvRow)
	            fp.close()
	        nextpage = soup.find('div',{'class':'activeLink right'})
	        if nextpage:
	            site  = s.get('https://www.property24.com.ng'+ nextpage.a['href'])
	#                 print('https://www.property24.com.ng'+ nextpage.a['href'])
	        else:
	            break



def checkAndUpdate(dbconn,dbname, tablename, s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
	categories = ['property-for-sale','property-to-rent']

	for category in categories:
	    site  = s.get('https://www.property24.com.ng/'+category+'?Page=1')
	    soup = BeautifulSoup(site.text,'html.parser')
	    while(True):
	    # for i in range(2):
	        soup = BeautifulSoup(site.text,'html.parser')
	        houses = soup.find_all('div',{'class':'propertyTileWrapper'})
	        for house in houses:
	            title =house.a['title']
	            link = 'https://www.property24.com.ng'+house.a['href']
	            price = house.span
	            if price:
	                price  = price.text.strip()
	                price = price.replace('₦ ','')
	                price  = price.replace(chr(160),"")
	                price = int(re.search(r'\d+', price).group())
	                # price  = price.replace(chr(160),"")
	                # price = price.replace('₦ ','')
	                if price == "":
	                    price = 0
	            else:
	                price = 0
	            address = house.find('div',{'class','left address'})
	            if address:
	                address = address.text
	                if address == "":
	                    address = "No Address Found"
	#                 else:
	#                     address = "No Address Found"
	            specs =house.find('div',{'class','left detailsBorder detailsContainer'})
	            if specs:
	                bed_bath = specs.find_all('span')
	                if house.find('img',{'src':'/Content/Images/BedTile.png?58c88b '}):
	                    bed =  bed_bath[0].text
	                    if house.find('img',{'src':'/Content/Images/BathTile.png?498121'}):
	                        bath =  bed_bath[1].text
	                else:
	                    bed =0
	                    if house.find('img',{'src':'/Content/Images/BathTile.png?498121'}):
	                        bath =  bed_bath[0].text
	                    else:
	                        bath=0

	                size = specs.select_one("span[class='sizeUnits']")
	                if size:
	                    size = size.text.strip().replace(chr(160),"")
	                else:
	                    size = 0
	            else:
	                bed =0
	                bath =0
	                size=0
	            csvRow = [title,address,price,bed,bath,size,link,formatted_date]
	            mySql_insert_query = """INSERT INTO Property24 ( Title, Address, Price, Beds, Bath, Area, URL, DateScrapped ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
	            checkUrlInDB(link,csvRow,tablename,dbconn,mySql_insert_query)

	        nextpage = soup.find('div',{'class':'activeLink right'})
	        if nextpage:
	            site  = s.get('https://www.property24.com.ng'+ nextpage.a['href'])
	#                 print('https://www.property24.com.ng'+ nextpage.a['href'])
	        else:
	            break


if __name__ == '__main__':
    s= Session()
    csvFileName = csv_path + 'Property24.csv'
    tablename = 'Property24'
    changes_tablename = 'Property24_Changes'
    CheckForDB()
    connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbusername,
                                         password=dbpassword)
    mySql_Create_Table_Query = """CREATE TABLE {} ( 
         Id int NOT NULL AUTO_INCREMENT,
         Title varchar(255) NOT NULL,
         Address varchar(255) NOT NULL,
         Price bigint NOT NULL,
         Beds Varchar(255) NULL,
         Bath varchar(255) NOT NULL,
         Area varchar(255) NOT NULL,
         URL varchar(255) NOT NULL,
         DateScrapped Date NOT NULL,
         PRIMARY KEY (Id)) """
    checkTable = checkTableExists(connection,tablename)
    if checkTable == False:
        print("No Table Found With Name {}".format(tablename))
        createTable(connection,mySql_Create_Table_Query,tablename)
        print("Data Scrapping Started...")
        scrapAll(csvFileName,s)
        print("All Data is Scrapped and saved in: {}".format(csvFileName))

        data = pd.read_csv(csvFileName)

        addDataToDB(data,dbname,tablename)
        print("Data Populated in Table: {}".format(tablename))

    else:
    	print("Table already Exist.\nChecking to updated results...")
    	checkAndUpdate(connection, dbname, tablename, s)    	
