import socket
import queue
import threading
import json
import sys
import pickle
import os

import functions

#Sockets
serversockets = {}

currentLeader = 5

def checkSocket( targetServer ):
    try:
        serversockets[targetServer].sendall( pickle.dumps("socketcheck") )
    except:
        tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tempSock.connect( (socket.gethostname(), portList[str(targetServer)]) )
        serversockets[targetServer] = tempSock

    return


def requestLeader():
    global currentLeader
    while True:
        targetServer = currentLeader - 1
        print(f'No response, requesting server {currentLeader-1} to become Leader')

        data = ("leader", "client", "")
        threading.Thread(target=functions.sendMessage, args =( serversockets[targetServer], data, True  )).start()
        serversockets[currentLeader].settimeout(10)

        try:
            serversockets[targetServer].recv(1024)
            currentLeader = targetServer
            return
        except socket.timeout:
            targetServer - 1

    return
    


if __name__ == "__main__":
    #Get Data
    with open("./config.json") as f:
        portList = json.load(f)

    while True:
        temp = input()
        if temp == "connect":
            break
        else:
            continue

    for key in portList:
        tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tempSock.connect( (socket.gethostname(), int(portList[key]) + 10)  )
        serversockets[int(key)] = tempSock

    print(f'Connected, Estimated Leader is Server {currentLeader}')

    #User Input
    while True:
        try:
            i = input()
            words = i.split()

            if words[0].lower() == "testmessage":
                words.pop(0)
                targetServer = int(words.pop(0))
                message = ' '.join( words )
                data = ("testmessage", "client", message)

                print(f'Sending message from Client to Server {targetServer}\n')
                checkSocket( targetServer )
                threading.Thread( target=functions.sendMessage, args=(serversockets[targetServer], data, True)).start()
            
            elif words[0].lower() == "operation":
                words.pop(0)
                op = words.pop(0)
                if op.lower() == "put":
                    data = (op, words.pop(0), words.pop(0) )
                elif op.lower() == "get":
                    data = (op, words.pop(0) )

                checkSocket( currentLeader )
                threading.Thread( target=functions.sendMessage, args=(serversockets[currentLeader], data, True)).start()
                print(f'Waiting for response...')

                try:
                    received = pickle.load( serversockets[currentLeader].recv(1024) )
                except socket.timeout:
                    data = ("leader", "client", "")
                    requestLeader()
                
                threading.Thread( target=functions.sendMessage, args=(serversockets[currentLeader], data, True)).start()
                received = pickle.load( serversockets[currentLeader].recv(1024) )
                print(f'Response from Server: {received[0]}')
                continue
            else:
                continue
        except KeyboardInterrupt:
            os._exit(0)
