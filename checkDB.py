'''
Module to confirm db is active and capture stats
'''

from neo4j.v1 import GraphDatabase, basic_auth

def checkTheDb():
    returnableSet = []
    #confim we can connect and if so, export the stats
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
    session = driver.session()
    try:
        results = session.run("match (a)-[c]-(b) return count(c) as relcount")
        for record in results:
            returnableSet.append(record['relcount'])
        results = session.run("match (a) return count(a) as nodecount")
        for record in results:
            returnableSet.append(record['nodecount'])
    except:
        print("Trouble executing the query")
        raise RuntimeError
    finally:
        session.close()

    return returnableSet

if __name__ == '__main__':
    checkTheDb()
    print(returnableSet)


