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


def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False


def createFile(filename,cols):
    isDirectory = os.path.isdir(csv_path)
    if isDirectory == False:
        os.mkdir(csv_path)
        print("{} directory is created".format(csv_path))
    
#     filename = "NiraBricks.csv"
#     li = ['Title','Address','Price','Beds','Bath','Area','URL']
    f1 = open(filename, "a", encoding='UTF-8')
    cols = pd.DataFrame(columns=cols)
    cols.to_csv(filename,sep=',', index=False)


def createTable(dbcon,mySql_Create_Table_Query,tablename):
    try:

        cursor = dbcon.cursor()
        result = cursor.execute(mySql_Create_Table_Query.format(tablename))
        backup_result = cursor.execute(mySql_Create_Table_Query.format(tablename+"_Changes"))

        print("Table {} Created Sucessfully".format(tablename))
        cursor.close()
    except mysql.connector.Error as error:
        dbcon.rollback()
        print("Failed to create table {}".format(tablename))



def populateTable(dbcon,tablename,csvFileName):
    data = pd.read_csv(csvFileName)
    data = data.drop_duplicates()
    current_Date = datetime.now()
    formatted_date = current_Date.strftime('%Y-%m-%d %H:%M:%S')
    data['Date'] = formatted_date
    cols = "`,`".join([str(i) for i in data.columns.tolist()])
    
    cursor = dbcon.cursor()
    for i,row in data.iterrows():
        sql = "INSERT INTO "+tablename+" (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
        dbcon.commit()
    cursor.close()

    

def getAllDataFromDatabase(dbname,tablename):
    db_connection_str = 'mysql+pymysql://root:root@localhost/'+ dbname
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql('SELECT * FROM '+tablename, con=db_connection)
    return df



def writeInCSV(filename,csvData):
    with open(filename, "a",newline="") as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerow(csvData)
    fp.close()



def addDataToDB(data,dbname,tablename):
    # data = pd.read_csv(csvFileName)
    db_connection_str = 'mysql+pymysql://' + dbusername + ':' + dbpassword + '@' + dbhost +'/'+ dbname
    db_connection = create_engine(db_connection_str)
    data.to_sql(con=db_connection, name=tablename, if_exists='append',index=False)




def checkUrlInDB(url,csvRow,tablename,dbconn,insertquery):

    query = "select * from {} where url = '{}'".format(tablename,url)
    cursor = dbconn.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    # mySql_insert_query = """INSERT INTO 2dotProperty ( Title, Address, Price, Beds, Baths, Toilets, URL, AgentName, AgentEmail, AgentPhone, DateScrapped ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    if records == []:
        cursor = dbconn.cursor( )
        cursor.execute(insertquery, csvRow)
        dbconn.commit()

        print("Data Inserted with url", url)


def CheckForDB():
    # import mysql.connector
    # dbname = 'property'
    dbcon = mysql.connector.connect(
      host=dbhost,
      user=dbusername,
      passwd=dbpassword
    )

    mycursor = dbcon.cursor()
    try:
        # mycursor.execute("CREATE DATABASE {}".format(dbname))
        mycursor.execute("CREATE DATABASE {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".format(dbname))

        
        print("Database {} created sucessfully".format(dbname))
    except mysql.connector.DatabaseError as e:
        print("DB {} already Exist".format(dbname))