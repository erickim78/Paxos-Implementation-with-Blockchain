import pickle
import time
import hashlib
import uuid


def writeList( l: list ):
    with open('blockchain.txt','wb') as f:
        pickle.dump(l, f)
    return

def readList( filename: str ) -> list:
    rlist = []
    with open(filename,'rb') as f:
        rlist = pickle.load(f)
    return rlist

def printList( l: list ):
    for item in l:
        print(item)
    return

def sendMessage( socket, data, isActive ):
    time.sleep(1)

    if isActive is True:
        try:
            socket.sendall( pickle.dumps( data ) )
        except:
            return
    #
    #else:
    #    print("This Network Link is Inactive\n")
        
    return 


