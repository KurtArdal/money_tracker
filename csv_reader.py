import csv
import sys
from os import getcwd
from os import listdir
from os.path import isfile, join
import datetime

import configparser
import mysql.connector

# Read config file
config = configparser.ConfigParser()
config.read('../Py_MT_config.ini')

# Connect to mysql
mydb = mysql.connector.connect(host = config['mysqlDB']['host'], 
user = config['mysqlDB']['user'], 
passwd = config['mysqlDB']['password'], 
db = config['mysqlDB']['database'])

# Setup cursor and queries
mycursor = mydb.cursor()
#ins_query = ("insert ignore into transak_import (konto_id, bokfort, rentedato, tekstkode, beskrivelse, belop, arkivref, motkonto) values (%s, %s, %s, %s, %s, %s, %s, %s)")

ins_query = ("INSERT INTO transak_import(konto_id, bokfort, rentedato, tekstkode, beskrivelse, belop, arkivref, motkonto) \
            SELECT * FROM (SELECT %s AS konto_id, %s AS bokfort, %s AS rentedato, %s AS tekstkode, %s AS beskrivelse, %s AS belop, %s AS arkivref, %s AS motkonto) AS tmp \
            WHERE NOT EXISTS \
            (SELECT konto_id, bokfort, rentedato, tekstkode, beskrivelse, belop, arkivref, motkonto FROM transak_import WHERE \
                konto_id=tmp.konto_id && \
                bokfort=tmp.bokfort && \
                rentedato=tmp.rentedato && \
                tekstkode=tmp.tekstkode && \
                beskrivelse=tmp.beskrivelse && \
                belop=tmp.belop && \
                arkivref=tmp.arkivref && \
                motkonto=tmp.motkonto)")

acc_query = ("select konto_id from kontoer where konto_navn = %s")# limit 1"

# Get path (current working directory)
mypath = getcwd()

# Read in filenames
files = [file for file in listdir(mypath) if (isfile(join(mypath, file)) and file.endswith(".csv"))]

# Check if there are any files
if len(files) <= 0:
    print("No files")
    sys.exit()

# Position to read to, -1 for start. 
pos = -1 

for file in files:
    with open(file, newline='\n') as csvFile:
        # Set acc_id to 0.
        acc_id = 0

        # Find the first - in the filename and get index (pos)
        if (file.find("-")) != -1:
            pos = (file.find("-"))
        
        # Retrieve filename from 0, up to the first -, and add it as array/list
        acc_name = [file[0:pos]]

        # Run account query and fetch result, and set acc_id to value from query
        mycursor.execute(acc_query, acc_name)
        result = mycursor.fetchall()
        for row in result:    
            acc_id = row[0]
        
        # Read csv file, and use delimiter
        csvReader = csv.reader(csvFile, delimiter='\t')

        # Skip first line (headers)
        next(csvReader)

        # Loop through the list in reverse. 
        # If there is , in the belop section, replace it with . to make mysql happy.
        for row in reversed(list(csvReader)):
            if row[4].find(',') != -1:
                row[4] = row[4].replace(",", ".")
            data = (acc_id,(datetime.datetime.strptime(row[0], '%d.%m.%Y').strftime('%Y-%m-%d')), (datetime.datetime.strptime(row[1], '%d.%m.%Y').strftime('%Y-%m-%d')), row[2], row[3], row[4], row[5], row[6]) 
            mycursor.execute(ins_query, data)
            mydb.commit()
            
mycursor.close()

mydb.close()
