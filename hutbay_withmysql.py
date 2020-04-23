from requests import Session
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
from config import *
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
from genaric import *
import re


# all basic methods

def findBedrooms(house):
    bed = house.find('i',{'class':'fa-bed'})
    if bed:
        bed = bed.parent.text.strip().replace(" bedrooms","")
        return bed
    else:
        return 0
    
def findBathrooms(house):
    bath = house.find('i',{'class':'fa-bath'})
    if bath:
        bath = bath.parent.text.strip().replace(" bathrooms","")
        return bath
    else:
        return 0

def findToilets(house):
    toilet = house.find('i',{'class':'fa-toilet'})
    if toilet:
        toilet = toilet.parent.text.strip().replace(" toilets","")
        return toilet
    else:
        return 0

def findAddress(house):
    address = house.find('i',{'class':'fa-map-marker'})
    if address:
        address = address.parent.text.strip()
        return address
    else:
        return "No Address Found"

def findPrice(house):
    price = house.find('a',{'class':'price'})
    if price:
        price = price.text.strip()
        price = price.replace("₦","")
        price  = price.replace(",","")
        price = price.replace("$","")
        if price == "call for price":
            return 0
        return price
    else:
        return 0
 


def checkAndUpdate(dbconn,dbname, tablename, s):
    print("Scrapping Started...")
    current_Date = datetime.now()
    formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')

    site  = s.get('https://www.hutbay.com/search?adverttypes=1%2c2&propertytypes=&query=&page=1&count=15')

    while(True):
    # for i in range(3):
        soup = BeautifulSoup(site.text,'html.parser')
        houses = soup.find_all('div',{'class':'recentLs'})
        for house in houses:
            url = "https://www.hutbay.com"+house.a['href']
            title = house.find('a',{'class':'title'}).text
            beds = findBedrooms(house)
            baths = findBathrooms(house)
            toilets = findToilets(house)
            address= findAddress(house)
            price = findPrice(house)
            
            subpagesoup = BeautifulSoup(s.get('https://www.hutbay.com'+ house.a['href']).text,'html.parser')
            td = subpagesoup.find_all('td')
            table_data = ""
            for tab in td:
                table_data = table_data + " " + tab.text.strip()
            try:
                reference = re.search('Reference: (.*) Advert', table_data)
                if reference == "":
                    reference = "No Reference Found"
                else:
                    reference = reference.group(1)

                date_added = re.search('Date Added: \d{2}-\w*-\d{4}', table_data)
                if date_added == "":
                    date_added = "No Date Found"
                    
                else:
                    date_added = date_added.group(0).replace("Date Added: ", "")
                    
            except:
                print('https://www.hutbay.com'+ house.a['href'])



            csvRow = [title,address,price,beds,baths,toilets,date_added,formatted_date,reference,url]

            mySql_insert_query = """INSERT INTO Hutbay ( Title, Address, Price, Beds, Bath, Toilet, DateAdded, DateScrapped, Reference, URL ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            checkUrlInDB(url,csvRow,tablename,dbconn,mySql_insert_query)
            # writeInCSV(csvFileName, csvRow)

        nextPage = soup.find('li',{'class':'PagedList-skipToNext'})
        if nextPage:
            site  = s.get('https://www.hutbay.com' + nextPage.a['href'])
        else:
            Print("All Data is Scrapped and saved in: " + csvFileName)

            break





# method to scrap all data from web and write a CSV
def scrapAll(csvFileName,s):
	print("Scrapping Started...")
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
	cols = ['Title', 'Address', 'Price', 'Beds', 'Bath', 'Toilet', 'DateAdded', 'DateScrapped', 'Reference' ,'URL']
	# data = pd.DataFrame(columns=['Title', 'Address', 'Price', 'Beds', 'Bath', 'Toilet', 'DateAdded', 'DateScrapped', 'Reference' ,'URL'])
	createFile(csvFileName,cols)
	site  = s.get('https://www.hutbay.com/search?adverttypes=1%2c2&propertytypes=&query=&page=21&count=15')
	while(True):
	# for i in range(3):
	    soup = BeautifulSoup(site.text,'html.parser')
	    houses = soup.find_all('div',{'class':'recentLs'})
	    for house in houses:
	        url = "https://www.hutbay.com"+house.a['href']
	        title = house.find('a',{'class':'title'}).text
	        beds = findBedrooms(house)
	        baths = findBathrooms(house)
	        toilets = findToilets(house)
	        address= findAddress(house)
	        price = findPrice(house)
	        
	        subpagesoup = BeautifulSoup(s.get('https://www.hutbay.com'+ house.a['href']).text,'html.parser')
	        td = subpagesoup.find_all('td')
	        table_data = ""
	        for tab in td:
	            table_data = table_data + " " + tab.text.strip()
	        try:
	            reference = re.search('Reference: (.*) Advert', table_data)
	            if reference == "":
	                reference = "No Reference Found"
	            else:
	                reference = reference.group(1)

	            date_added = re.search('Date Added: \d{2}-\w*-\d{4}', table_data)
	            if date_added == "":
	                date_added = "No Date Found"
	                
	            else:
	                date_added = date_added.group(0).replace("Date Added: ", "")
	                
	        except:
	            print('https://www.hutbay.com'+ house.a['href'])



	        csvRow = [title,address,price,beds,baths,toilets,date_added,formatted_date,reference,url]
	        writeInCSV(csvFileName, csvRow)

	    nextPage = soup.find('li',{'class':'PagedList-skipToNext'})
	    if nextPage:
	        site  = s.get('https://www.hutbay.com' + nextPage.a['href'])
	    else:
	    	Print("All Data is Scrapped and saved in: " + csvFileName)

	    	break
    # return data

if __name__== '__main__':
    s= Session()
    csvFileName = csv_path + 'HutBay.csv'
    tablename = 'Hutbay'
    changes_tablename = 'Hutbay_Changes'
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
             Beds int NOT NULL,
             Bath int NOT NULL,
             Toilet int NOT NULL,
             DateAdded varchar(255) NOT NULL,
             DateScrapped Date NOT NULL,
             Reference varchar(255) NOT NULL,
             URL varchar(255) NOT NULL,
             PRIMARY KEY (Id)) """

    checkTable = checkTableExists(connection, tablename)
    if checkTable == False:
        print("No Table Found With Name {}".format(tablename))
        createTable(connection,mySql_Create_Table_Query,tablename)
        scrapAll(csvFileName,s)
        data = pd.read_csv(csvFileName)

        data = data[['Title','Address','Price','Beds','Bath','Toilet','DateAdded','DateScrapped','Reference','URL']]
        data['DateScrapped'] = pd.to_datetime(data['DateScrapped'])
        addDataToDB(data,dbname,tablename)
        print("All data is scrapped and populated in the table: {}".format(tablename))
    else:
        print("Table already Exist.\nChecking to updated results...")
        checkAndUpdate(connection, dbname, tablename, s)
 