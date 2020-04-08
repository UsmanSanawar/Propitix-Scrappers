from requests import Session
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
import csv
from config import *
import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
from genaric import *


def getMaxPage(s):
    
    site  = s.get('https://nairabricks.com/listings/searchproperties?page=5&type=All+categories')
    soup = BeautifulSoup(site.text,'html.parser')
    pages = soup.find('p',{'class':'pagelist'})
    all_pages = pages.find_all('a')
    max_page = all_pages[-2].text.strip()
    maxpage = int(max_page)
    return maxpage


def scrapAll(csvFileName,s):
    current_Date = datetime.now()
    formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S') 
    cols = ['Title','Address','Price','Beds','Bath','Area','URL', 'DateScrapped']

    createFile(csvFileName,cols)
    maxPage = getMaxPage(s)
    print("Total Number of pages : ",maxPage)
    
    for i in range(maxPage):
        site  = s.get('https://nairabricks.com/listings/searchproperties?page='+ str(i) +'&type=All+categories')
        soup = BeautifulSoup(site.text,'html.parser')
        raw = soup.find('div',{"id":"listing-list"})
        if raw:
            products = raw.find_all('div',{'class':'col-md-6'})
            for product in products:
                if product.a:
                    url =product.a['href']
                    title = product.h4.text
                    details = product.p.text.replace('\n',' ').split()
                    price = details[0]
                    beds = details[1]
                    bath = details[3]
                    area_size = details[6]+ " "+ details[7]
                    address = product.find('a',{'class':'caption-address'}).text.strip()
                    csvRow = [title, address, price, beds, bath, area_size,url,formatted_date]
                    writeInCSV(csvFileName,csvRow)




def checkAndUpdate(dbconn,dbname, tablename, s):
    current_Date = datetime.now()
    formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
    maxPage = getMaxPage(s)
    print("Total Number of pages : ",maxPage)
    
    for i in range(maxPage):
        site  = s.get('https://nairabricks.com/listings/searchproperties?page='+ str(i) +'&type=All+categories')
        soup = BeautifulSoup(site.text,'html.parser')
        raw = soup.find('div',{"id":"listing-list"})
        if raw:
            products = raw.find_all('div',{'class':'col-md-6'})
            for product in products:
                if product.a:
                    url =product.a['href']
                    title = product.h4.text
                    details = product.p.text.replace('\n',' ').split()
                    price = details[0]
                    beds = details[1]
                    bath = details[3]
                    area_size = details[6]+ " "+ details[7]
                    address = product.find('a',{'class':'caption-address'}).text.strip()
                    csvRow = [title, address, price, beds, bath, area_size,url,formatted_date]
                    mySql_insert_query = """INSERT INTO NairaBricks ( Title, Address, Price, Beds, Bath, Area, URL, DateScrapped ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
                    checkUrlInDB(url,csvRow,tablename,dbconn,mySql_insert_query)



if __name__== '__main__':
    s= Session()
    csvFileName = csv_path + 'NairaBricks.csv'
    tablename = 'NairaBricks'
    changes_tablename = 'NairaBricks_Changes'
    dbname = 'property'
    connection = mysql.connector.connect(host='localhost',
                                         database='property',
                                         user='root',
                                         password='')
    mySql_Create_Table_Query = """CREATE TABLE {} ( 
             Id int NOT NULL AUTO_INCREMENT,
             Title varchar(255) NOT NULL,
             Address varchar(255) NOT NULL,
             Price int ,
             Beds varchar(255) ,
             Bath varchar(255) ,
             Area varchar(255),
             URL varchar(255) NOT NULL,
             DateScrapped Date NOT NULL,
             PRIMARY KEY (Id)) """

    checkTable = checkTableExists(connection,'NairaBricks')
    
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


