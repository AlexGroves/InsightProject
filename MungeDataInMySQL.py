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

#___________________________________________Open MySQL connection_____________________________________________________
mySqlDatabaseName = raw_input("Enter the name of your MySQL database.")

con = mdb.connect('localhost', 'root', '', mySqlDatabaseName)

#______________________________Define templates, payment plans, and website tools_____________________________________
templateList = [] # *** list of templates here. ***
planList = [] # *** list of plans here. ***
toolList = [] # *** list of event categories (tools) here. ***

# Creating a user centric table. It lists the id, template type, plan type, average daily clicks, age, first date seen.
#___________________________________Create user-centric attribute table_______________________________________________
def UserAttributes(con,templateList,planList):
# Creating a super table that is the union of the different template tables. Has the
# user's template choice and also their plan.
#   Input:
#                  con = MySQL connection.
#         templateList = list of template tables. (string)
#             planList = list of payment plan types. (string)
#-----------------------------------------------------------------------------------
    with con:
        cur = con.cursor()

        sqlCommand1 = """DROP TABLE IF EXISTS %s; \
                         CREATE TABLE %s (iKey INT PRIMARY KEY AUTO_INCREMENT, \
                                          yearMonthDay DATE, id INT"""

        sqlCommand2 = "INSERT INTO %s (yearMonthDay,id"
        sqlCommand3a = "(SELECT max(DATE(time)),id"
        sqlCommand4 = "SELECT max(yearMonthDay),id"

        # Add in each template and plan name.
        tempPlanInitStr = ''
        tempPlanCallStr = ''
        ifPlanStr = ''

        for tempOrPlan in templateList+planList:
            tempPlanInitStr += "," + tempOrPlan + " INT"
            tempPlanCallStr += "," + tempOrPlan
            # Creates a plan dummy variable (0 if the user does not have that plan, 1 if she does).
            if tempOrPlan in planList:
                ifPlanStr += ',if(plan="' + tempOrPlan + '",1,0)'

        # This loop creates a union of all template tables.
        sqlCommand3 = ''
        templateDummyList = []
        for i in xrange(len(templateList)):
            sqlCommand3 += sqlCommand3a

            # Putting in dummy variables for templates 
            sqlCommand3 += ",%s" * len(templateList)
            iDumsList = ["'0'"]*len(templateList)
            iDumsList[i] = "'1'"
            templateDummyList += iDumsList

            # Add in plan dummy creation strings.
            sqlCommand3 += ifPlanStr

            # Define the template table we are adding to the union.
            sqlCommand3 += " FROM " + templateList[i] + " GROUP BY id)"
            # Check to see if it's the last template table in the list.
            if i < len(templateList)-1:
                sqlCommand3 += " UNION "
            else:
                sqlCommand3 += ";"

        sqlCommand1 += tempPlanInitStr + ");"
        sqlCommand2 += tempPlanCallStr + ") "
        sqlCommand3 = sqlCommand3 % tuple(templateDummyList)
        sqlCommand4 += tempPlanCallStr + """ FROM %s \
                                            GROUP BY id;"""

        # Putting most of the stuff above together to create a table of user ids, templates used,
        # and plan type. This table could have users who are repeated for different templates.
        sqlCommandA1 = sqlCommand1 % ("superTable","superTable")
        sqlCommandA2 = (sqlCommand2 % "superTable") + sqlCommand3
        cur.execute(sqlCommandA1)
        cur.execute(sqlCommandA2)

        # Since users may have multiple templates, this removes all but the most recent
        # template for every user.
        sqlCommandB1 = sqlCommand1 % ("templateTypes","templatetypes")
        sqlCommandB2 = (sqlCommand2 % "templateTypes") + (sqlCommand4 % "superTable")
        cur.execute(sqlCommandB1)
        cur.execute(sqlCommandB2)

        # Remove the date column.
        sqlCommand = "ALTER TABLE templateTypes DROP yearMonthDay;"
        cur.execute(sqlCommand)

        # Clean up: drop superTableA.
        sqlCommand = "DROP TABLE superTable;"
        cur.execute(sqlCommand)

        #______________________________________User activity table_________________________________________________
        # Create userActivity table to hold static measures for users: most recent date of activity,
        # id, total clicks, age, and plan.
        sqlCommand = """DROP TABLE IF EXISTS userActivity;
                        CREATE TABLE userActivity (iKey INT PRIMARY KEY AUTO_INCREMENT, \
                                                   lastDayActive DATE,id INT,clicks FLOAT, \
                                                   age INT,plan VARCHAR(50));"""
        cur.execute(sqlCommand)

        # Insert into userActivity table from userClicks.
        sqlCommand = """INSERT INTO userActivity (lastDayActive,id,clicks,age,plan) \
                       (SELECT max(DATE(time)),id,count(id), DATEDIFF(max(DATE(time)),min(DATE(time)))+1,plan \
                        FROM userClicks \
                        GROUP BY id);"""
        cur.execute(sqlCommand)

        # Create a table that calculates the average daily clicks for each user.
        sqlCommand1 = """DROP TABLE IF EXISTS users; \
                         CREATE TABLE users (iKey INT PRIMARY KEY AUTO_INCREMENT, \
                                             id INT,avUse FLOAT,age INT,lastDayActive DATE"""
        sqlCommand2 = "INSERT INTO users (id,avUse,age,lastDayActive"
        sqlCommand3 = "SELECT id,clicks/age,age,lastDayActive" + ifPlanStr

        for planType in planList:
            sqlCommand1 += "," + planType + " INT"
            sqlCommand2 += "," + planType

        sqlCommand1 += ");"
        sqlCommand2 += ") "
        sqlCommand3 += """ FROM userActivity \
                           GROUP BY id;"""

        # Feed these commands into MySQL.
        cur.execute(sqlCommand1)
        cur.execute(sqlCommand2 + sqlCommand3)

        #_________________________Join activeUsers table with templateTypes table___________________________________
        # Join the activeUsers table with the templateTypes table.
        sqlCommand1 = """DROP TABLE IF EXISTS userAttributes; \
                         CREATE TABLE userAttributes (iKey INT PRIMARY KEY AUTO_INCREMENT, \
                         id INT, avUse FLOAT, age INT, lastDayActive DATE""" + tempPlanInitStr + ");"
        cur.execute(sqlCommand1)

        sqlCommand2 = "INSERT INTO userAttributes (id,avUse,age,lastDayActive" + tempPlanCallStr + ") "
        sqlCommand3 = "SELECT users.id,avUse,age,lastDayActive"

        for tempPlan in templateList + planList:
            if tempPlan in planList:
                sqlCommand3 += ",users." + tempPlan
            else:
                sqlCommand3 += "," + tempPlan

        sqlCommand3 += """ FROM users \
                         LEFT JOIN templateTypes on users.id = templateTypes.id;"""

        cur.execute(sqlCommand2 + sqlCommand3)

#_____________________________________Create website feature use table_________________________________________
def WebsiteFeatureTable(con,toolList):
# Creates a table of website features (tools) used and who (user id) is using them.
#
#     Inputs:
#              con = MySQL connection.
#         toolList = list of tool tables. (string)
#-----------------------------------------------------------------------------------
    with con:
        cur = con.cursor()

        sqlCommand1 = """DROP TABLE IF EXISTS toolTable; \
                        CREATE TABLE toolTable (iKey INT PRIMARY KEY AUTO_INCREMENT, \
                                                yearMonthDay DATE,id INT,editor INT"""

        sqlCommand2 = "INSERT INTO toolTable (yearMonthDay,id,editor"
        sqlCommand3a = "(SELECT DATE(time),id,editor_counter"
        toolInitStr = ''
        toolCallStr = ''
        toolDumsList = []
        sqlCommand3 = ''
        for i in xrange(len(toolList)):
            sqlCommand3 += sqlCommand3a
            toolInitStr += "," + toolList[i] + " INT"
            toolCallStr += "," + toolList[i]
            
            # Creating tool dummy variables.
            iDumsStr = ",'%d'" * len(toolList)
            iDumsList = [0] * len(toolList)
            iDumsList[i] = 1
            toolDumsList += iDumsList
            sqlCommand3 += iDumsStr
            
            sqlCommand3 += "FROM " + toolList[i] + ")"
            if i < len(toolList) - 1:
                sqlCommand3 += " UNION "
            else:
                sqlCommand3 += ";"

        sqlCommand1 += toolInitStr + ");"
        sqlCommand2 += toolCallStr + ")"
        sqlCommand3 = sqlCommand3 % tuple(toolDumsList)
            
        cur.execute(sqlCommand1)
        cur.execute(sqlCommand2 + sqlCommand3)

#____________________________________________Calling functions__________________________________________________
if __name__ == "__main__":
    UserAttributes(con,templateList,planList)
    WebsiteFeatureTable(con,toolList)