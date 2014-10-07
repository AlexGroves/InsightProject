# coding: utf-8

import re
import os
import os.path
import glob
import shutil
import pymysql as mdb
import datetime
import numpy as np
import pandas as pd
import time
import json
import matplotlib.pylab as plt
import sklearn
import scipy
import sys

#____________________________________Open connection to MySQL________________________________________________
mySqlDatabaseName = raw_input("Enter the name of your MySQL database.")

con = mdb.connect('localhost', 'root', '', mySqlDatabaseName)

#________________________________List of tools and user attributes___________________________________________
toolList = [] # *** tool list + editor ***

userAttriList = [] # *** user attributes - editor ***

#____________________________________________________________________________________________________________
def DataDf(con,toolList,userAttriList):
# Creates a new table called activeUserAttributes which contains only users who have been active in the last
# month. Then joins this table to the toolTable and recalculates tool use in terms of weekly lifetime averages.
#   Inputs:
#                 con = MySQL connection.
#            toolList = List of website features (tools).
#       userAttriList = List of user attributes.
#   Output:
#             dataDf = pandas dataframe with active user attributes and average weekly tool use data.
#-----------------------------------------------------------------------------------------
    with con:
        cur = con.cursor()

        # Command creates the activeUserAttributes table.
        sqlCommand1a = """DROP TABLE IF EXISTS activeUserAttributes; \
                         CREATE TABLE activeUserAttributes (iKey INT PRIMARY KEY AUTO_INCREMENT, \
                                                            id INT"""
        sqlCommand1b = "INSERT INTO activeUserAttributes (id"
        sqlCommand1c = "SELECT id"

        # This command inner joins the activeUserAttributes table with the toolTable by user id.
        # The inner join means that users who do not use any tools will still be in the table. 
        # If it were a regular join (intersection) they would be thrown out.
        sqlCommand2 = "SELECT activeUserAttributes.id"

        # Looping over the combined userAttriList and toolList to avoid having to spell everything out.
        toolAttCallStr = ''
        joinedStr = ''
        for toolOrAtt in userAttriList + toolList:
            if toolOrAtt in userAttriList:
                sqlCommand2 += "," + toolOrAtt
                toolAttCallStr += "," + toolOrAtt

                # Average use is a float, everything else is an int.
                if toolOrAtt == "avUse":
                    sqlCommand1a += "," + toolOrAtt + " FLOAT"

                else:
                    sqlCommand1a += "," + toolOrAtt + " INT"

            # The tool use is calculated as the weekly average use over the lifetime (age) of the user.
            else:
                sqlCommand2 += ",7*" + toolOrAtt + "/age AS " + toolOrAtt

                # The editor use exists as an overall count in the userAttribute table instead of a table 
                # of its own like the remainder of the tools. Hence the "sum" command instead of "count".
                if toolOrAtt == "editor":
                    joinedStr += ",sum(" + toolOrAtt + ") AS " + toolOrAtt

                else:
                    joinedStr += ",count(" + toolOrAtt + ") AS " + toolOrAtt


        # Putting the first three sub-commands back together and executing.
        sqlCommand1a += ");"
        sqlCommand1b += toolAttCallStr + ") "
        sqlCommand1c += toolAttCallStr + """ FROM userAttributes \
                                            WHERE lastDayActive >= DATE_SUB('2014-09-10', INTERVAL 28 DAY) \
                                            GROUP BY id;""" #CURDATE()
        cur.execute(sqlCommand1a)
        cur.execute(sqlCommand1b + sqlCommand1c)

        # Putting the second command back together and executing using pandas.
        sqlCommand2 += """ FROM activeUserAttributes \
                              INNER JOIN (SELECT id"""
        sqlCommand2 += joinedStr
        sqlCommand2 += """ FROM toolTable GROUP BY id) table1 \
                          ON activeUserAttributes.id = table1.id;"""
        dataDf = pd.read_sql_query(sqlCommand2,con)

        # Replace any NaNs with 0.
        dataDf = dataDf.replace(np.nan,0)

        return dataDf

#____________________________________________________________________________________________________________
def NormalizeData(data):
# Normalize each column of data by the maximum value of the column.
#   Input:
#             data = all user data in numpy array (#users x #tools+features+variables)
#   Ouptup:
#       normedData = data normalized
#-----------------------------------------------------------------------------------------

    (n,m) = data.shape
    
    # Make a matrix where each column is a vector of that columns maximum value.
    maxVector = np.array([np.amax(data,0)])
    maxMatrix = np.ones((n,1)).dot(maxVector)
    
    normedData = data/maxMatrix
    normedData = np.nan_to_num(normedData)
    
    return normedData

#____________________________________________________________________________________________________________
def FindToolUsers(notUsingArray,userDistanceVector,user):
# Finds the set of users who use at least one of the tools in kNeighborsTools.
# This will be the set of users we compare vec1 to.
#   Inputs:
#           notUsingArray = array of usage of tools that "user" does not employ.
#      userDistanceVector = vector of distances from "user" to all other users.
#
#   Outputs:
#      toolUserToolsArray = array of tool usage by customers who actually make use of the additional tools.
#      userDistanceVector = customers who do not use any additional tools are removed.
#-----------------------------------------------------------------------------------------

    toolUserToolsArray = notUsingArray[np.sum(notUsingArray,1)>0]
    userDistanceVector = userDistanceVector[np.sum(notUsingArray,1)>0]
    
    # If no customer uses more tools than "user" this error is thrown.
    if np.sum(userDistanceVector) == 0:
        print "No one uses more tools than customer %d" % user
    
    return toolUserToolsArray,userDistanceVector

#____________________________________________________________________________________________________________
def KnnEstimate(toolUserToolsArray,userDistanceVector,k):
# Takes in the "training" data, the set of tools used for prediction,
# the user-vector for whom we are making the prediction, and the number,
# of nearest neighbors we are looking at.
# data, kNeighborsTools, and vec1 should be normalized before being input.
#   Inputs:
#       toolUserToolsArray = neighbor tool use data. (n x # of extra tools) (array)
#       userDistanceVector = vector of distances to neighbors who use additional tools. (n x 1)
#
#   Outputs:
#           suggestedTools = weights for each suggested tool in the index location of the tool.
#-----------------------------------------------------------------------------------------

#___________________Suggested Tools___________________
# Calculate predictions based on average of k neighbors (right now this is not a weighted average. It could be).
    
    # Sort the distance vector from closest to furthest neighbor. Make index of these values.
    neighborsIndexLocation = np.array(range(len(userDistanceVector)))
    neighborsDistanceIndex = dict(zip(userDistanceVector,neighborsIndexLocation))
    nKeysSorted = neighborsDistanceIndex.keys()
    nKeysSorted.sort()
    
    # Initialize the tools matrix. 'm'>=1 is the number of tools we are looking through to suggest.
    (n,m) = toolUserToolsArray.shape
    tools = np.zeros((k,m))
    
    for i in range(k):
        # nKeysSorted[i] is the distance between i and "user".
        # neighborsDistanceIndex[nKeysSorted[i]] is the index for i in toolUserToolsArray.
        neighborIndex = neighborsDistanceIndex[nKeysSorted[i]]
        tools[i] = toolUserToolsArray[neighborIndex]   
    suggestedTools = np.sum(tools,0)/k
    
    return suggestedTools

#____________________________________________________________________________________________________________
def ToolSuggestion(dataDf,userList,userAttriList,toolList,k=30):
# Makes website feature (tool) suggestions for the users in userList.
# Inputs:
#          dataDf = data frame with all user attributes and tools.
#        userList = list of user ids for whom you would like to make suggestions.
#   userAttriList = list of user attributes (template type, plan type, average use, age).
#        toolList = list of tools (analytics,appStore,fb,linkedIn,rewards,so on).
#
# Outputs:
#  suggestionList = list of suggestions, ordered in the same way as the userList.
#_________________________________________________________________________________________
#___________________________Read in Data_________________________
#    dataDf = DataDf()
    tic = time.time()
    #______________________Create User Index_________________________
    # Creating a dictionary of user ids and their corresponding row number so that they
    # can be found quickly.
    userId = dataDf["id"].values
    userIndex = np.array(range(len(userId)))
    userIndexDic = dict(zip(userId,userIndex))

    # Drop the Id column so it doesn't screw things up during the distance measurements.
    dataDf = dataDf.drop("id",1)

    #_______________Find Attribute and Tool Locations________________
    attIndexList = []
    toolIndexList = []

    for variable in userAttriList:
        attIndexList += [list(dataDf.keys()).index(variable)]

    for variable in toolList:
        toolIndexList += [list(dataDf.keys()).index(variable)]

    #_______________________Normalize the data_______________________
    dataArray = dataDf.values
    dataArray = dataArray.astype(np.float32)
    normedDataArray = NormalizeData(dataArray)

    #___________Measure Pairwise Distance Between All Users__________
    distanceArray = sklearn.metrics.pairwise.pairwise_distances(normedDataArray,metric='cosine')

    #___________________Split the Data in Two________________________
    # Split the data into user attributes and tools.
    userAttArray = normedDataArray[:,attIndexList]
    toolArray = normedDataArray[:,toolIndexList]

    #______________Find unused tools for each user__________________
    suggestedTools = {}

    for user in userList:
        # Making toolList into an array so that it's easier to grab multiple
        # elements at the same time.
        toolNameArray = np.array(toolList)
        unusedToolNameArray = toolNameArray[toolArray[userIndexDic[user],:]==0]

        # Find the tools "user" uses and does not use.
        notUsingArray = toolArray[:,toolArray[userIndexDic[user],:]==0]
        usedToolArray = toolArray[:,toolArray[userIndexDic[user],:]>0]

        # If the user uses all tools, then we have nothing to suggest.
        n,m = notUsingArray.shape
        if m >0:
            # Vector of pairwise distances for "user"
            userDistanceVector = distanceArray[userIndexDic[user],1:]

            # Delete user row from the original arrays.
            notUsingArray = np.delete(notUsingArray,(userIndexDic[user]),axis=0)

            # Find users who actually use the tools that "user" does not. Comparing "user"
            # to people who also do not use other tools is pointless here.
            toolUserToolsArray,userDistanceVector = FindToolUsers(notUsingArray,userDistanceVector,user)

            # Find the suggested tools:
            #k = 60
            suggestedToolsWeights = KnnEstimate(toolUserToolsArray,userDistanceVector,k)
            #unorderedSuggestions = zip(,)
            #unorderedSuggestions.sort(reverse=True)
            suggestedTools[user] = [x for y, x in sorted(zip(suggestedToolsWeights[suggestedToolsWeights>0], 
                                                             unusedToolNameArray[suggestedToolsWeights>0])
                                                         ,reverse=True)]

    return suggestedTools

#____________________________________________________________________________________________________________
if __name__ == "__main__":
    dataDf = DataDf(con,toolList,userAttriList)
    #userList = list(dataDf[id])
    suggestedTools = ToolSuggestion(dataDf,userList,userAttriList,toolList)
    
    with open('data.json', 'wb') as fp:
        json.dump(suggestedTools, fp)