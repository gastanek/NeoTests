'''
Module for capturing command line responses to run the test
'''

from cmd import Cmd
import multiprocessing

#Define return values, set defaults
#[0] = delete existing db?
#[1] = number of nodes to use?
#[2] = number of properties per node?
#[3] = number of txns? (this will be extended in the future)
#[4] = number of processes? (parallelize the txn workload)

callResponses = [1, 100, 4, 1000, 2]

def getTheResponse(number, question, valid):
    #get a response to the question from the user
    response = input(question)
    settable = 0
    try:
        settable = int(response)
    except ValueError:
        print("Invalid response, please use an integer value")
    if settable in valid:
        callResponses[number] = int(response)
    elif number == 1 or number == 3:
        #this is an int, apply to the set
        callResponses[number] = int(response)
    else:
        print("Invalid response...using default of " + str(callResponses[number]))

def getResponses():
    getTheResponse(0, 'Should we delete the existing DB? (1=yes, 0=no)> ', [1, 0])
    getTheResponse(1, 'How many nodes would you like to create?> ', [])
    getTheResponse(2, 'How many properties should those nodes have?> ', range(1,6))
    getTheResponse(3, 'How many relationships would you like to create?> ', [])
    getTheResponse(4, 'How many processes should I use?> ', range(multiprocessing.cpu_count()))
    return callResponses

if __name__ == '__main__':
    getTheResponse(0, 'Should we delete the existing DB? (1=yes, 0=no)> ', [1, 0])
    getTheResponse(1, 'How many nodes would you like to create?> ', [])
    getTheResponse(2, 'How many properties should those nodes have?> ', range(1,6))
    getTheResponse(3, 'How many relationships would you like to create?> ', [])
    getTheResponse(4, 'How many processes should I use?> ', range(multiprocessing.cpu_count()))
    print(callResponses)
