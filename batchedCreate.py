'''
Created to test batch records insert into Neo
Needs improvement to timing
'''

import random
import time
import multiprocessing
import sys
from neo4j.v1 import GraphDatabase, basic_auth, CypherError

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
names = ["Al", "Jo", "Ted", "Bob", "Sarah", "Suzy"]
sessions = []

#txn execution process
def executeTxns(top, batchsize, ppid):
    print("Starting " + str(top[0]) + " pool txns for process " + str(ppid))
    i = 0
    cursession = sessions[ppid]

    #divide the top by the batchsize so we only loop to create at most the max requested
    loops = top[0]/batchsize[0]
    print(str(loops))
    while i < loops:
        #our batch is are sets in array
        j = 0
        propbatch = []
        while j < int(batchsize[0]):
            idprops = [i+j, names[random.randint(0, 5)]]
            propbatch.append(idprops)
            j += 1
        statement = "UNWIND " + str(propbatch) + " as row CREATE (a:Person {id: row[0], name: row[1]})"
        with cursession.begin_transaction() as txn:
            try:
                result = txn.run(statement)
                summary = result.consume()
                txn.success = True
            except CypherError:
                raise RuntimeError("Cypher didn't work")
            except:
                print("Transcaction failed")
                txn.success = False
        cursession.close()
        i += 1
    print("Done with pool txns for " + str(ppid))

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print(len(sys.argv))
        print("Invalid number of arguments.  Provide a number of txns and number of processes per txn to run and the batch size")
        sys.exit(2)

    '''
    Incoming commandline arguments
    [0] = Script name (ignore)
    [1] = the total number of nodes to write
    [2] = how many processes to split this up over
    [3] = the batch size per txn sent to the server
    '''

    top = int(sys.argv[1])
    processes = int(sys.argv[2])
    batch = int(sys.argv[3])

    sessions = []
    for n in range(processes):
        sessions.append(driver.session())

    start = time.time()
    print(str(start) + ' is the start time')

    multiprocessing.set_start_method('fork')

    print(str(multiprocessing.cpu_count()) + ' is the total CPU count')
    topper = [top]
    batched = [batch]
    workers = []
    for num in range(processes):
        p = multiprocessing.Process(target=executeTxns, args=(topper, batched, num))
        workers.append(p)
        p.start()

    for worker in workers:
        worker.join()

    end = time.time()
    eltime = end - start
    print(str(eltime) + ' seconds to complete batched version')



