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
import json



def scrapAll(csvFileName,s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
	cols = ['Title','Address','Price','Beds','Baths','Toilets','URL','AgentName','AgentEmail','AgentPhone', 'DateScrapped']
	createFile(csvFileName,cols)

	url = 'https://backendapi.2dotsproperties.com/q/search/?page=1'

	while(True):
	# for i in range(5):
	    r = requests.get(url)
	    json_data = json.loads(r.text)
	    houses = json_data['posts']
	#     if houses == []:
	#         break;
	    for house in houses:
	        price = house['price']
	        title = house['title']
	        address = house['location']
	        beds = house['bedrooms']
	        bath = house['bathrooms']
	        toilets = house['toliets']
	        agent_name = house['agent']['name']
	        agent_email = house['agent']['email']
	        agent_phone = house['agent']['phone']
	        if agent_name is None:
	        	agent_name = "No Agent Found"
	        if agent_email is None:
	        	agent_email = "No Agent Email Found"
	        if agent_phone is None:
	        	agent_phone = "No phone Num found"
	        url = "https://2dotsproperties.com/properties/" + house['permalink']

	        csvRow = [title, address, price,beds,bath, toilets, url, agent_name, agent_email, agent_phone,formatted_date]
	        writeInCSV(csvFileName,csvRow)

	    links = json_data['links']
	    if links['next_page'] == '/?page=':
	        break;
	    url = 'https://backendapi.2dotsproperties.com/q/search' + links['next_page']
	    print(url)



def checkAndUpdate(dbconn,dbname, tablename, s):
	current_Date = datetime.now()
	formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
	url = 'https://backendapi.2dotsproperties.com/q/search/?page=1'

	while(True):
	# for i in range(5):
	    r = requests.get(url)
	    json_data = json.loads(r.text)
	    houses = json_data['posts']
	#     if houses == []:
	#         break;
	    for house in houses:
	        price = house['price']
	        title = house['title']
	        address = house['location']
	        beds = house['bedrooms']
	        bath = house['bathrooms']
	        toilets = house['toliets']
	        agent_name = house['agent']['name']
	        agent_email = house['agent']['email']
	        agent_phone = house['agent']['phone']
	        if agent_name is None:
	        	agent_name = "No Agent Found"
	        if agent_email is None:
	        	agent_email = "No Agent Email Found"
	        if agent_phone is None:
	        	agent_phone = "No phone Num found"
	        url = "https://2dotsproperties.com/properties/" + house['permalink']

	        csvRow = [title, address, price,beds,bath, toilets, url, agent_name, agent_email, agent_phone,formatted_date]

	        mySql_insert_query = """INSERT INTO 2dotProperty ( Title, Address, Price, Beds, Baths, Toilets, URL, AgentName, AgentEmail, AgentPhone, DateScrapped ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

	        checkUrlInDB(url,csvRow,tablename,dbconn,mySql_insert_query)
	        # writeInCSV(csvFileName,csvRow)

	    links = json_data['links']
	    if links['next_page'] == '/?page=':
	        break;
	    url = 'https://backendapi.2dotsproperties.com/q/search' + links['next_page']
	    print(url)



if __name__ == '__main__':
    s= Session()
    csvFileName = csv_path + '2dotProperty.csv'
    tablename = '2dotProperty'
    changes_tablename = '2dotProperty_Changes'
    dbname = 'property'
    connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbusername,
                                         password=dbpassword)

    #  in this query datatype of some colums is set to text because the data got is too inconsistent it cannot be fit in varchar and each row is a way different from other
    #  if this script didnt work run this mysql qurry in sql console --->ALTER DATABASE database_name CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    mySql_Create_Table_Query = """CREATE TABLE {} ( 
         Id int NOT NULL AUTO_INCREMENT,
         Title text NOT NULL,
         Address text NOT NULL,
         Price bigint NOT NULL,
         Beds Varchar(255) ,
         Baths varchar(255),
         Toilets varchar(255),
         URL text NOT NULL,
         AgentName text,
         AgentEmail text,
         AgentPhone text,
         DateScrapped varchar(255) NOT NULL,
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

