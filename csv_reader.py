import csv
from os import getcwd
from os import listdir
from os.path import isfile, join
import datetime

import configparser
import mysql.connector

config = configparser.ConfigParser()
config.read('../Py_MT_config.ini')

mydb = mysql.connector.connect(host = config['mysqlDB']['host'], 
user = config['mysqlDB']['user'], 
passwd = config['mysqlDB']['password'], 
db = config['mysqlDB']['database'])

mycursor = mydb.cursor()
ins_query = ("insert ignore into transak_import (konto_id, bokfort, rentedato, tekstkode, beskrivelse, belop, arkivref, motkonto) values (%s, %s, %s, %s, %s, %s, %s, %s)")
acc_query = ("select konto_id from kontoer where konto_navn=%s")

mypath = getcwd()

files = [file for file in listdir(mypath) if (isfile(join(mypath, file)) and file.endswith(".csv"))]
acc_name = ""

my_list = []

for file in files:
    with open(file, newline='\n') as csvFile:
        csvReader = csv.reader(csvFile, delimiter='\t')
        next(csvReader)
        for row in reversed(list(csvReader)):
            if row[4].find(',') != -1:
                row[4] = row[4].replace(",", ".")
            data = ('1',(datetime.datetime.strptime(row[0], '%d.%m.%Y').strftime('%Y-%m-%d')), (datetime.datetime.strptime(row[1], '%d.%m.%Y').strftime('%Y-%m-%d')), row[2], row[3], row[4], row[5], row[6]) 
            mycursor.execute(ins_query, data)
            mydb.commit()
            
mycursor.close()

mydb.close()
