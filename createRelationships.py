'''
Module to create requested nodes in the db
This creates nodes that we'll connect with relationships
'''

import random
import time
import multiprocessing
import sys
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
names = []
sessions = []

#txn execution process
def executeTxns(rels, nodes, ppid):
    #rels is the number of relationships we create per process
    #nodes is the number of nodes in the db
    print("Creating " + str(rels[0]) + " relationships with process " + str(ppid))
    i = 0
    cursession = sessions[ppid]
    while i < rels[0]:
        #match two random ids and the create the relationship between them
        statement = "MATCH (a:Person {id:" + str(random.randint(1,nodes[0])) + "}) with a MATCH (b:Person {id:" + str(random.randint(1,nodes[0])) + "}) with a,b CREATE (a)-[:FOLLOWS]->(b)"
        with cursession.begin_transaction() as txn:
            try:
                result = txn.run(statement)
                summary = result.consume()
                txn.success = True
            except:
                print("Transcaction failed")
                txn.success = False
        cursession.close()
        i += 1
    print("Done with pool txns for " + str(ppid))

def runRelGen(dimensions):

    # Dimensions
    # [0] = delete existing db?
    # [1] = number of nodes to use?
    # [2] = number of properties per node?
    # [3] = number of rels?
    # [4] = number of processes?

    #get number of txns
    nodeCount = dimensions[1]
    relCount = dimensions[3]
    processes = dimensions[4]

    for n in range(processes):
        sessions.append(driver.session())

    rels = [relCount/processes]
    nodes = [nodeCount]
    workers = []
    for num in range(processes):
        p = multiprocessing.Process(target=executeTxns, args=(rels, nodes, num))
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print(len(sys.argv))
        print("Invalid number of arguments.  Provide a number of txns and number of processes per txn to run.")
        sys.exit(2)

    top = int(sys.argv[1])
    props = int(sys.argv[2])
    processes = int(sys.argv[3])

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
    propper = [props]
    workers = []
    for num in range(processes):
        p = multiprocessing.Process(target=executeTxns, args=(topper, propper, num))
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()

    end = time.time()
    eltime = end - start
    print(str(eltime) + ' seconds to complete the graph')



