import random
import time
import multiprocessing
import sys
from neo4j.v1 import GraphDatabase, basic_auth

#txn execution process
def executeTxns(top, ppid):
    print("Starting " + str(top[0]) + " pool txns for process " + str(ppid))
    i = 0
    cursession = sessions[ppid]
    while i < top[0]:
        parameters = {'id': random.randint(1, 3500), 'name': names[random.randint(0, 5)],
                      'idto': random.randint(1, 3500),
                      'nameto': names[random.randint(0, 5)]}
        with cursession.begin_transaction() as txn:
            try:
                result = txn.run("MERGE (a:Person {id: {id}, name: {name}}) "
                        "MERGE (b:Person {id: {idto}, name: {nameto}})"
                        "MERGE (a)-[:FOLLOWS]->(b)", parameters)
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

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print(len(sys.argv))
        print("Invalid number of arguments.  Provide a number of txns and number of processes per txn to run.")
        sys.exit(2)

    top = int(sys.argv[1])
    processes = int(sys.argv[2])

    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
    names = ['Tod', 'Jen', 'Frank', 'Sarah', 'Alan', 'Andy']

    sessions = []
    for n in range(processes):
        sessions.append(driver.session())

    # clear the db
    session = driver.session()
    session.run("match (a)-[c]-(b) delete a,c,b")
    session.close()

    start = time.time()
    print(str(start) + ' is the start time')

    multiprocessing.set_start_method('fork')

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



