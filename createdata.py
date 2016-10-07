'''
Updated this script to simply slam in nodes as overall throughput test
but this is still operating as single statement per transaction
'''

import random
import time
import multiprocessing
import sys
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
names = ["Al", "Jo", "Ted", "Bob", "Sarah", "Suzy"]
sessions = []

#txn execution process
def executeTxns(top, ppid):
    print("Starting " + str(top[0]) + " pool txns for process " + str(ppid))
    i = 0
    cursession = sessions[ppid]
    while i < top[0]:
        parameters = {'id': random.randint(1, 200000), 'name': names[random.randint(0, 5)]}
                      #'idto': random.randint(1, 3500),
                      #'nameto': names[random.randint(0, 5)]
        with cursession.begin_transaction() as txn:
            try:
                result = txn.run("CREATE (a:Person {id: {id}, name: {name}}) "
                        #"MERGE (b:Person {id: {idto}, name: {nameto}})"
                        #"MERGE (a)-[:FOLLOWS]->(b)"
                        , parameters)
                summary = result.consume()
                #for record in result:
                    #do absolutely nothing but loop through them
                #    print(record)
                txn.success = True
            except:
                print("Transcaction failed")
                txn.success = False
        cursession.close()
        i += 1
    print("Done with pool txns for " + str(ppid))

def runDataGen(dimensions):

    # Dimensions
    # [0] = delete existing db?
    # [1] = number of nodes to use?
    # [2] = number of properties per node?
    # [3] = number of txns?
    # [4] = number of processes?

    #get number of txns
    top = dimensions[3]
    processes = dimensions[4]

    for n in range(processes):
        sessions.append(driver.session())

    topper = [top]
    workers = []
    for num in range(processes):
        p = multiprocessing.Process(target=executeTxns, args=(topper, num))
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print(len(sys.argv))
        print("Invalid number of arguments.  Provide a number of txns and number of processes per txn to run.")
        sys.exit(2)

    top = int(sys.argv[1])
    processes = int(sys.argv[2])

    sessions = []
    for n in range(processes):
        sessions.append(driver.session())

    # clear the db
    session = driver.session()
    session.run("match (a)-[c]-(b) delete a,c,b")
    session.close()

    start = time.time()
    print(str(start) + ' is the start time')

    #multiprocessing.set_start_method('fork')

    print(str(multiprocessing.cpu_count()) + ' is the total CPU count')
    topper = [top]
    workers = []
    for num in range(processes):
        p = multiprocessing.Process(target=executeTxns, args=(topper, num))
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()

    end = time.time()
    eltime = end - start
    print(str(eltime) + ' seconds to complete parameterized version')



