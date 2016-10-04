'''
Simple module for dropping the data in the db before we create more data
'''

from neo4j.v1 import GraphDatabase, basic_auth

def deleteDatabase():
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"))
    session = driver.session()
    session.run("match (a)-[c]-(b) delete a,c,b")
    session.close()

if __name__ == '__main__':
    #runnable as separate command line
    deleteDatabase()