'''
Main runnable python script to create and test data
'''

import createdata, createNodes, createRelationships
import commandlineconvo
import checkDB
import cleanupdb
import timer

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

    if responses[0] == 1:
        #we have been asked to delete the db
        if dbstats[1] > 100000:
            print("You've got a bit of a big DB, this may take a moment")
            cleanupdb.deleteDatabase()
            print("All done clearing out your DB...you are ready to start fresh.")
        else:
            cleanupdb.deleteDatabase()

    #now we know what to do, time to execute
    now = timer.startTime()
    #createdata.runDataGen(responses)
    createNodes.runDataGen(responses)
    createRelationships.runRelGen(responses)
    later = timer.endTime()
    print(str(later - now) + " seconds building that graph")
    print("We're all done here.  Now we can do other things.")