import random
import time
import multiprocessing
from neo4j.v1 import GraphDatabase, basic_auth


def createPeople(number):
    #Create a number of users so we can avoid merge
    #use an incrementing set of id's
    i=0
    cursession = sessions[0]
    #cursession = driver.session()
    while i < number:
        parameters = {'id': i}
        with cursession.begin_transaction() as txn:
            try:
                result = txn.run("CREATE (a:Person {id: {id}})", parameters)
                result.consume()
                txn.success = True
            except:
                print("Transaction failed creating a user")
                txn.success = False
        i += 1
    print(str(number) + " Users are created")
    cursession.close()

#txn execution process
def executeRelationships(top, people, ppid):
    print("Starting " + str(top[0]) + " pool txns for process " + str(ppid))
    i = 0
    cursession = sessions[ppid]
    while i < top[0]:
        parameters = {'id': random.randint(1, people),
                      'idto': random.randint(1, people)}
        with cursession.begin_transaction() as txn:
            try:
                result = txn.run("MERGE (a:Person {id: {id}}) "
                        "MERGE (b:Person {id: {idto}})"
                        "MERGE (a)-[:FOLLOWS]->(b)", parameters)
                #success = result.consume()
                txn.success = True
            except:
                print("Relationship transcaction failed")
                txn.success = False
        i += 1
    #moved the call to close the session ahead of closing the process using the session
    #cursession.close()
    print("Done with pool txns for " + str(ppid))

if __name__ == '__main__':

    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
    names = ['Tod', 'Jen', 'Frank', 'Sarah', 'Alan', 'Andy']
    totalpeople = 50
    top = 2500
    processes = 2

    sessions = []
    for n in range(processes):
        sessions.append(driver.session())

    #clear the db
    results = sessions[0].run("match (a)-[c]-(b) delete a,c,b")
    results.consume()

    '''
    testsession = driver.session()
    results = testsession.run("match (a)-[c]-(b) delete a,c,b")
    results.consume()
    testsession.close()
    '''
    start = time.time()
    print(str(start) + ' is the start time')

    multiprocessing.set_start_method('fork')

    print(str(multiprocessing.cpu_count()) + ' is the total CPU count')
    topper = [top]
    peoples = [totalpeople]
    workers = []

    createPeople(totalpeople)

    for num in range(processes):
        p=multiprocessing.Process(target=executeRelationships, args=(topper, totalpeople, num))
        #multiprocessing.Process(target=executeTxns(top)).start()
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()


    end = time.time()
    eltime = end-start
    print(str(eltime) + ' seconds to complete parameterized version')

    #close off our sessions
    for proc in sessions:
        if proc.healthy==True:
            print(proc.healthy)
            driver.recycle(proc)
            proc.close()
        else:
            print("this session isn't healthy")
