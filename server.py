import socket
import queue
import threading
import json
import sys
import pickle
import os
import hashlib
import os.path
from os import path

import block
import functions


#Initialize Data Structures
blockchain = []
q = queue.Queue()
kvstore = {}

#Socket Data Structures
portList = {}
serversockets = {}
clientsockets = {}
socketstatus = {}

#Paxos
currentLeader = 5
currentBallot = (0,0,0)
currentBallotNum = 0
accepted = 0
promises = 0


def failProcess():
    for key in serversockets:
        serversockets[key].close()
    os._exit(0)

    return


def checkSocket( targetServer ):
    try:
        serversockets[targetServer].sendall( pickle.dumps("socketcheck") )
    except:
        try:
            tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tempSock.connect( (socket.gethostname(), portList[str(targetServer)]) )
            serversockets[targetServer] = tempSock
        except:
            return
        return

    return


def listenForServers( address, port ):
    with socket.socket( socket.AF_INET, socket.SOCK_STREAM ) as s:
        s.bind( (address, port) )
        s.listen()

        while True:
            conn, addr = s.accept()
            threading.Thread( target=handleServer, args=(conn,) ).start()

    return


def handleServer( conn ):
    global currentBallot
    global currentBallotNum
    global currentLeader
    while True:
        try:
            data = conn.recv(1024)
        except:
            continue
        try:
            received = pickle.loads(data)
        except EOFError:
            continue

        if received[0] == "testmessage":
            print(f'Message Received from Server {received[1]}: {received[2]}')
        elif received[0] == "put" or received[0] == "get":
            if currentLeader == int(PID):
                q.put( received )
            else:
                checkSocket(currentLeader)
                threading.Thread( target=functions.sendMessage, args=(serversockets[currentLeader], received, socketstatus[currentLeader] ) ).start()
        elif received[0] == "accept":
            threading.Thread( target=sendAccepted, args=(conn, received) ).start()
        elif received[0] == "decide":
            print(f'Received Decide from Server {received[1][2]}')
            acceptval = received[2]
            acceptval.decided = True
            blockchain.append(acceptval)
            currentop = acceptval.operation
            if currentop[0] == "put":
                kvstore[currentop[2]] = currentop[3]
            print("A Block has been decided")
        elif received[0] == "prepare":
            recballot = received[2]
            if recballot > currentBallot:
                currentBallot = recballot
                currentBallotNum = recballot[0]
                data = ("promise", currentBallot)
                print(f'Sending a promise to server {received[1]}')
                threading.Thread( target=functions.sendMessage, args=(serversockets[int(received[1])], data, socketstatus[int(received[1])] ) ).start()
        elif received[0] == "claimleader":
            currentLeader = int(received[1])
            print(f'Server {currentLeader} has become the Leader')
        else:
            "ERROR, UNKNOWN MESSAGE"

    return


def listenForClients( address, port ):
    with socket.socket( socket.AF_INET, socket.SOCK_STREAM ) as s:
        s.bind( (address, port) )
        s.listen()

        while True:
            conn, addr = s.accept()
            threading.Thread( target=handleClient, args=(conn,) ).start()
            
    return


def handleClient( conn ):
    global currentBallot
    global currentBallotNum
    while True:
        data = conn.recv(1024)
        received = pickle.loads(data)

        if received[0] == "testmessage":
            print(f'Message Received from a Client: {received[2]}')
        elif received[0] == "put" or received[0] == "get":
            print(f'Received an operation from a Client')
            if currentLeader == int(PID):
                q.put( received )
            else:
                threading.Thread( target=functions.sendMessage, args=(clientsockets[int(received[1])], ("wronglead", PID, currentLeader), True) ).start()
                threading.Thread( target=functions.sendMessage, args=(serversockets[currentLeader], received, socketstatus[currentLeader] ) ).start()
        elif received[0] == "leader":
            print(f'Received request to become Leader')
            data = ("leaderattempt", PID)
            threading.Thread( target=functions.sendMessage, args=(conn, data, True) ).start()

            currentBallotNum += 1
            currentBallot = (currentBallotNum,len(blockchain),int(PID))
            data = ("prepare", PID, currentBallot )
            for key in serversockets:
                try:
                    threading.Thread( target=functions.sendMessage, args=(serversockets[key], data, socketstatus[key]) ).start()
                    threading.Thread( target=awaitPromises, args=(serversockets[key],) ).start()
                    print(f'Sending ballot {currentBallot} to server {key}')
                except:
                    print(f'Cant reach server {key}')
            threading.Thread( target=waitingForLead ).start()
        else:
            "ERROR, UNKNOWN MESSAGE"
    return


def handleOperations():
    global accepted
    global blockchain
    while True:
        currentop = q.get(block=True, timeout=None)
        tempblock = block.Block( currentop )
        if len(blockchain) > 0:
            prevBlock = blockchain[ len(blockchain)-1 ]
            tempblock.hashpointer = hashlib.sha256( prevBlock.toBytes() ).hexdigest()
        
        data = ("accept", (currentBallotNum, len(blockchain), int(PID)), tempblock)
        accepted = 2
        for key in serversockets:
            checkSocket(key)
            threading.Thread( target=functions.sendMessage, args=(serversockets[key],data, socketstatus[key]) ).start()
            threading.Thread( target=awaitAccepted, args=(serversockets[key],) ).start()
            print(f'Sending accept to server {key}')

        while accepted > 0:
            continue
        print("Got Enough Accepted, a block has been decided")

        rval = ""
        tempblock.decided = True
        blockchain.append(tempblock)
        if currentop[0] == "put":
            kvstore[currentop[2]] = currentop[3]
            rval = ("opcomplete", PID, "ack")
        elif currentop[0] == "get":
            if currentop[2] in kvstore:
                rval = ("opcomplete", PID, kvstore[currentop[2]])
            else:
                rval = ("opcomplete", PID, "NO_KEY")
        
        threading.Thread( target=functions.sendMessage, args=(clientsockets[int(currentop[1])], rval, True) ).start()

        data = ("decide", (currentBallotNum, len(blockchain), int(PID)), tempblock)
        for key in serversockets:
            try:
                checkSocket(key)
                threading.Thread( target=functions.sendMessage, args=(serversockets[key], data, socketstatus[key]) ).start()
                print(f'Sending decide to server {key}')
            except:
                continue

        functions.writeList( blockchain )
        


def awaitAccepted( s ):
    global accepted
    if accepted <= 0:
        return

    received = pickle.loads( s.recv(1024) )
    if received[0] == "accepted":
        accepted -= 1
    return


def awaitPromises( s ):
    global promises
    if promises <= 0:
        return

    received = pickle.loads( s.recv(1024) )
    print(f'Received a promise for ballot {currentBallot}')
    if received[0] == "promise":
        promises -= 1
    return

def waitingForLead():
    global promises
    global currentLeader

    while promises > 0:
        continue


    currentLeader = int(PID)
    data = ("claimleader", PID)
    
    for key in serversockets:
        try:
            checkSocket(serversockets[key])
            threading.Thread( target=functions.sendMessage, args=(serversockets[key], data, socketstatus[key]) ).start()
        except:
            print(f'Cant Reach Server {key}')
    print("BEFORE THE SENDS")
    for key in clientsockets:
        threading.Thread( target=functions.sendMessage, args=(clientsockets[key], data, True) ).start()

    print("I have become the leader")
       
    return

def sendAccepted( s, data ):
    global blockchain
    global kvstore
    if data[1] >= currentBallot:
        newdata = ("accepted", data[1], data[2] )
        acceptval = data[2]

        if data[1][1] > len(blockchain):
            blockchain = functions.readList( "blockchain.txt" )
            kvstore = {}
            for b in blockchain:
                if b.operation[0] == "put":
                    kvstore[b.operation[2]] = b.operation[3]

        threading.Thread( target=functions.sendMessage, args=(s, newdata, socketstatus[data[1][2]]) ).start()
        print(f'Sending accepted to server {data[1][2]}')
    return


if __name__ == "__main__":
    #Get Command Line Args
    PID = sys.argv[1]
    currentBallot = (0,0,int(PID))

    #Get Data
    with open("./config.json") as f:
        portList = json.load(f)

    with open("./clientconfig.json") as f:
        clientPortList = json.load(f)

    myPort = portList[PID]
    del portList[PID]


    #Listen for Servers
    threading.Thread( target=listenForServers, args=( socket.gethostname(), myPort) ).start()

    #Listen for Clients
    threading.Thread( target=listenForClients, args=( socket.gethostname(), myPort+10) ).start()


    #Connect to other Servers
    while True:
        temp = input()
        if temp == "connect":
            break
        else:
            continue

    for key in portList:
        tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tempSock.connect( (socket.gethostname(), portList[key]) )
        serversockets[int(key)] = tempSock
        socketstatus[int(key)] = True
        print(f'Connected to Server {key}')

    for key in clientPortList:
        tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tempSock.connect( (socket.gethostname(), clientPortList[key]) )
        clientsockets[int(key)] = tempSock
        print(f'Connected to Client {key}')

    threading.Thread(target=handleOperations).start()

    if path.exists("blockchain.txt") and os.path.getsize("blockchain.txt"):
        blockchain = functions.readList( "blockchain.txt" )
        for b in blockchain:
            if b.operation[0] == "put":
                kvstore[b.operation[2]] = b.operation[3]

    #User Input
    while True:
        try:
            i = input()
            words = i.split()

            if words[0].lower() == "testmessage":
                words.pop(0)
                targetServer = int(words.pop(0))
                message = ' '.join( words )

                print(f'Sending message from Server {PID} to Server {targetServer}\n')
                checkSocket(targetServer)
                data = ("testmessage", PID, message)
                threading.Thread( target=functions.sendMessage, args=(serversockets[targetServer], data, socketstatus[targetServer] ) ).start()
            
            elif words[0].lower() == "testbroadcast":
                words.pop(0)
                message = ' '.join( words )

                print(f'Sending message from Server {PID} to All Servers\n')
                for key in serversockets:
                    checkSocket( key )
                    data = ("testmessage", PID, message) 
                    threading.Thread( target=functions.sendMessage, args=(serversockets[key], data, socketstatus[key] ) ).start()
            
            elif words[0].lower() == "faillink":
                words.pop(0)
                targetServer = int(words.pop(0))
                socketstatus[targetServer] = False
                print(f'Connection from Server {PID} to Server {targetServer} has failed\n')

            elif words[0].lower() == "fixlink":
                words.pop(0)
                targetServer = int(words.pop(0))
                socketstatus[targetServer] = True
                print(f'Connection from Server {PID} to Server {targetServer} has been fixed\n')

            elif words[0].lower() == "failprocess":
                failProcess()

            elif words[0].lower() == "printblockchain":
                print("PRINTING BLOCKCHAIN:")
                functions.printList( blockchain )
            
            elif words[0].lower() == "printkvstore":
                print("PRINTING KVSTORE:")
                print( kvstore )

            elif words[0].lower() == "printqueue":
                print("PRINTING QUEUE")
                print(q.queue)

            else:
                continue
        except KeyboardInterrupt:
            os._exit(0)
