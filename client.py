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


def checkSocket( targetServer ):
    try:
        serversockets[targetServer].sendall( pickle.dumps("socketcheck") )
    except:
        tempSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tempSock.connect( (socket.gethostname(), portList[str(targetServer)]) )
        serversockets[targetServer] = tempSock

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

        print(f'Connected to Server {key}')

    #User Input
    while True:
        try:
            i = input()
            words = i.split()

            if words[0].lower() == "testmessage":
                words.pop(0)
                targetServer = int(words.pop(0))
                message = ' '.join( words )

                print(f'Sending message from Client to Server {targetServer}\n')
                checkSocket( targetServer )
                threading.Thread( target=functions.sendMessage, args=(serversockets[targetServer],("testmessage", "client", message), True)).start()
            else:
                continue
        except KeyboardInterrupt:
            os._exit(0)
