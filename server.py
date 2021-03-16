import socket
import queue
import threading
import json
import sys
import pickle
import os
import block
import hashlib

import functions


#Initialize Data Structures
blockchain = []
q = queue.Queue()
kvstore = {}

#Socket Data Structures
portList = {}
serversockets = {}
socketstatus = {}

#Paxos
currentLeader = 5


def failProcess():
    for key in serversockets:
        serversockets[key].close()
    os._exit(0)

    return


def checkSocket( targetServer ):
    try:
        serversockets[targetServer].sendall( pickle.dumps("socketcheck") )
    except:
        tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tempSock.connect( (socket.gethostname(), portList[str(targetServer)]) )
        serversockets[targetServer] = tempSock

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
    while True:
        data = conn.recv(1024)
        try:
            recieved = pickle.loads(data)
        except EOFError:
            continue

        if recieved[0] == "testmessage":
            print(f'Message Recieved from Server {recieved[1]}: {recieved[2]}')
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
    while True:
        data = conn.recv(1024)
        recieved = pickle.loads(data)

        if recieved[0] == "testmessage":
            print(f'Message Recieved from a Client: {recieved[2]}')
        else:
            "ERROR, UNKNOWN MESSAGE"
    return


if __name__ == "__main__":
    #Get Command Line Args
    PID = sys.argv[1]

    #Get Data
    with open("./config.json") as f:
        portList = json.load(f)

    myPort = portList[PID]
    del portList[PID]


    #Listen for Servers
    threading.Thread( target=listenForServers, args=( socket.gethostname(), myPort) ).start()


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

    
    #Listen for Clients
    threading.Thread( target=listenForClients, args=( socket.gethostname(),myPort+10) ).start()


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
                threading.Thread( target=functions.sendMessage, args=(serversockets[targetServer],("testmessage", PID, message), socketstatus[targetServer] ) ).start()
            
            elif words[0].lower() == "testbroadcast":
                words.pop(0)
                message = ' '.join( words )

                print(f'Sending message from Server {PID} to All Servers\n')
                for key in serversockets:
                    checkSocket( key )
                    threading.Thread( target=functions.sendMessage, args=(serversockets[key],("testmessage", PID, message), socketstatus[key] ) ).start()
            
            elif words[0].lower() == "operation":
                # TO DO
                continue
            
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
                print(q)

            else:
                continue
        except KeyboardInterrupt:
            os._exit(0)
