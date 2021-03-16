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


def listenForServers( address, port ):
    with socket.socket( socket.AF_INET, socket.SOCK_STREAM ) as s:
        s.bind( (address, port) )
        s.listen()

        while True:
            conn, addr = s.accept()
            threading.Thread( target=handleServer, args=(conn,) ).start()

    return


def handleServer( conn ):
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
        elif received[0] == "opcomplete":
            print(f'Response from Server: {received[2]}')
        elif received[0] == "wronglead":  #PLACEHOLDER
            currentLeader = int(received[2])
            print(f'Forwarded to correct Leader: Server {received[2]}')
        elif received[0] == "claimleader":
            currentLeader = int(received[1])
            print(f'Server {currentLeader} has become the Leader')
        else:
            "ERROR, UNKNOWN MESSAGE"

    return


def requestLeader():
    global currentLeader
    targetServer = currentLeader
    while True:
        if targetServer == 1:
            targetServer = 5
        else:
            targetServer = targetServer - 1

        print(f'No response, requesting Server {targetServer} to become Leader')

        data = ("leader", PID)
        threading.Thread(target=functions.sendMessage, args =( serversockets[targetServer], data, True  )).start()
        serversockets[currentLeader].settimeout(10)

        try:
            received = pickle.loads( serversockets[targetServer].recv(1024) )
            if received[0] == "leaderattempt":
                print(f'Server {targetServer} is attempting to become the leader')
            return
        except:
            continue

    return
    


if __name__ == "__main__":
    PID = sys.argv[1]

    #Get Data
    with open("./config.json") as f:
        portList = json.load(f)
    with open("./clientconfig.json") as f:
        clientPortList = json.load(f)

    myPort = clientPortList[PID]

    threading.Thread( target=listenForServers, args=( socket.gethostname(), myPort) ).start()


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
                data = ("testmessage", PID, message)

                print(f'Sending message from Client to Server {targetServer}\n')
                checkSocket( targetServer )
                threading.Thread( target=functions.sendMessage, args=(serversockets[targetServer], data, True)).start()
            
            elif words[0].lower() == "operation":
                words.pop(0)
                op = words.pop(0)
                if op.lower() == "put":
                    data = (op, PID, words.pop(0), words.pop(0) )
                elif op.lower() == "get":
                    data = (op, PID, words.pop(0) )

                try:
                    checkSocket( currentLeader )
                    threading.Thread( target=functions.sendMessage, args=(serversockets[currentLeader], data, True)).start()
                    print(f'Waiting for response...')
                except:
                    requestLeader()
                    
            else:
                continue
        except KeyboardInterrupt:
            os._exit(0)
