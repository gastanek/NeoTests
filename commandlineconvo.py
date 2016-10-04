'''
Module for capturing command line responses to run the test
'''

from cmd import Cmd

#Define return values, set defaults
#[0] = delete existing db?
#[1] = number of txns? (this will be extended in the future)
#[2] = number of processes? (parallelize the txn workload)

callResponses = ['yes', 1000, 2]

def getTheResponse(number, question, valid):
    #get a response to the question from the user
    response = input(question)
    if response in valid:
        callResponses[number] = response
    else:
        print("invalid response...using default")

if __name__ == '__main__':
    getTheResponse(0, 'Should we delete the existing DB?', ['yes', 'no'])
