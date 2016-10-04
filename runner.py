'''
Main runnable python script to create and test data
'''

import createdata
import commandlineconvo
import checkDB

if __name__ == '__main__':

    #check if we need to delete the database first
    print("Welcome, one moment while I check your current Neo4j stats...")
    dbstats = checkDB.checkTheDb()
    if dbstats[1] > 0:
        #at least 1 node exists in the DB
        print("Looks like you currently have " + str(dbstats[1]) + " nodes and " + str(dbstats[0]) + " relationships in your Neo4j DB.")
    else:
        print("Looks like your current DB is empty.  I didn't find anything there.")

    #interactive command line conversation
    responses = commandlineconvo.getResponses()
    print(str(responses))

    #now we know what to do, time to execute
    createdata.runDataGen(responses)
