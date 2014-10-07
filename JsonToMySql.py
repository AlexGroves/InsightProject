
# coding: utf-8

import re
import os
import os.path
import glob
import shutil
import pymysql as mdb
import datetime
import json
import time
import math
from pprint import pprint

#_________________________________________________________________________________________________________________
# Define user properties, event categories, and events to keep track of.

propertiesSet = {} # put properties here.

categoryDic = {} # put events and associated categories here.

eventTypes = [] # put event categories here.

# MySQL table column names and value types. standList is for the event tables, and shortList is for the
# user centric data.
standList = [ ['time','TIMESTAMP'], ['id','INT'], ['plan','VARCHAR(50)'], ['sign_in_count','INT'],
             ['editor_counter','INT']]
shortList = [('time','TIMESTAMP'), ('id','INT'), ('plan','VARCHAR(50)')]

#____________________________________Open MySQL Connection________________________________________________________
# The data base is called 'strikinglydb'

mySqlDatabaseName = raw_input("Enter the name of your MySQL database.")

con = mdb.connect('localhost', 'root', '', mySqlDatabaseName)
cur = con.cursor()

#_________________________________________________________________________________________________________________
def CreateLetterIndex(indices):
# The text file split routine in Terminal indexes the new small files with letters appended on
# the end of the file name instead of numbers. This file creates the appropriate letter indexed
# file name.
# Input:
#      indicies = index number for the file
#
# Output:
#      letterIndex = the appropriate letter index
#---------------------------------------------------------------------------
    indices = indices + 1.
    characters = ['a','b','c','d','e','f','g','h','i','j','k','l','m',
                  'n','o','p','q','r','s','t','u','v','w','x','y','z']
    
    firstLetter = math.ceil(indices/26)
    
    # There must be a cooler way of doing this...
    if indices%26 == 0:
        secondLetter = 'z'
    else:
        secondLetter = indices%26
    
    letterIndex = characters[int(firstLetter)]+characters[int(secondLetter)]
    
    return letterIndex
#_________________________Creating a dictionary of tables to be created in MySQL__________________________________
bTabDict ={}
for types in eventTypes:
    bTabDict[types] = standList

#_____________________________________Initializing tables in MySQL________________________________________________
# Creating the MySQL commands that create the event tables.
for tab, cols in bTabDict.items():
    with con:
        cur.execute("DROP TABLE IF EXISTS "+tab)
        COMMAND = "CREATE TABLE "+tab+"(iKey INT PRIMARY KEY AUTO_INCREMENT "
        for column in cols:
            COMMAND += ", "+column[0]+" "+column[1]
        COMMAND += ")"     
        #print COMMAND
        cur.execute(COMMAND);

# Creating the MySQL command to create the table that contains every click for every user.
with con:
    cur.execute("DROP TABLE IF EXISTS userClicks")
    
    COMMAND = "CREATE TABLE userClicks (iKey INT PRIMARY KEY AUTO_INCREMENT "
    for propPair in shortList:
        COMMAND += ",%s %s" % propPair
    COMMAND += ")"
    
    cur.execute(COMMAND)

#___________________________Filtering and grouping data, then feeding into MySQL__________________________________
# This loop iterates through the data files, figures out which alphabetic index corresponds to the numeric index
# (1 = filename_aa, 2 = filename_ab,...,27 = filename_ba, so on). Opens this file and reads through line by line.
# Each line corresponds to a click event and is formatted as a dictionary of values and dictionaries.
# - If the event is among the categories defined above in categoryDic then all the properties in standList are
#   pulled and deposited into the appropriate MySQL table.
# - Even if it is not in categoryDic, the properties in shortList are pulled, this information is aggregated into a
#   very long string (which is a MySQL command), and is all dumped into MySQL when the last line of the file is read.

path = raw_input("Enter the path for the data files")

for letterIndex in xrange(177,0,-1): # Number of data files.    
    t = time.time()
    fileName = path + '/segment_' + CreateLetterIndex(float(letterIndex))
    ifile = open(fileName)

    # Counting the number of events we see (lines in ifile).
    eventcount = 0
    valuesString = ""

    with con:

        for line in ifile:
            # load the line from ifile. This line is in json format. Each line loads in as a dictionary with two
            # keys we care about. The first value is the event name, the second entry is properties, which is
            # another dictionary.
            d = json.loads(line)

            if "id" in d['properties'].keys():
                eventcount += 1

                # Need to add a comma at the end of valueString to separate the different values, but
                # only when there are other values there already. Also, do not want a comma at the very
                # end of valueString, this would cause problems in MySQL.
                columns = shortList
                if eventcount > 1:
                    valuesString += ","

                valuesString += "('" + str(d['properties']['id']) 
                valuesString += "','" + str(datetime.datetime.fromtimestamp(d['properties']['time']))
                if "plan" in d['properties'].keys():
                    valuesString += "','" + str(d['properties']['plan']) + "')"
                else:
                    valuesString += "','')"
                    
                if d['event'] in categoryDic.keys():
                    #Insert into table in order 
                    Table = categoryDic[d['event']]
                    cols = standList
                    stop = False

                    for col in cols: 
                        if col[0] not in d['properties']: stop = True
                    if(stop): continue

                    # INSERT INTO table_name(property_name1,property_name2,...
                    COMMAND = "INSERT INTO "+Table+"("

                    for col in cols[:-1]: 
                        label = col[0]
                        COMMAND += label+", "

                    # ,last_property_name) VALUES (...
                    label = cols[-1][0]   
                    COMMAND += label+" ) VALUES ("

                    # ...property_value1, variable_type, property_value2, variable_type,...
                    for col in cols[:-1]:
                        val = d['properties'][col[0]]
                        if col[0] == "time": val = "'"+str(datetime.datetime.fromtimestamp(val))+"'"
                        elif col[1] == "TIMESTAMP": val = "'"+str(val)+"'"
                        elif "VARCHAR" in col[1]: val = "'"+str(val)+"'"
                        else: val = str(val)
                        COMMAND += val+", "

                    # ...,last_property_value, variable_type)
                    val = d['properties'][cols[-1][0]]
                    if cols[-1][0] == "time": val = "'"+str(datetime.datetime.fromtimestamp(val))+"'"
                    elif cols[-1][0] == "TIMESTAMP": val = "'"+str(val)+"'"

                    elif "VARCHAR" in cols[-1][1]: val = "'"+val+"'"

                    else: val = str(val)

                    COMMAND += val+")"
                    cur.execute(COMMAND)  

    COMMAND = "INSERT INTO userClicks (id,time,plan) VALUES " + valuesString
    cur.execute(COMMAND)
    print letterIndex, time.time() - t