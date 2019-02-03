
import Queue
import SocketServer
import json
import sys
import threading

from sseclient import SSEClient

HOST, PORT = "52.202.156.203", 9000
#HOST, PORT = "127.0.0.1", 9002

q = Queue.Queue(10)
count = 0

class MyTCPHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        global q
        global count

        while True:

            item = q.get(True)
            data = item.data
            out = data.encode("utf-8").strip() + '\n'
            count = count + 1

            print(str(count) + ': ' + out)
            self.request.sendall(out)

server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)

server_thread = threading.Thread(target=server.serve_forever)
server_thread.setDaemon(True)
server_thread.start()

def wiki():
    global q
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    messages = SSEClient(url)
    for msg in messages:
        if len(msg.data) > 0:
            print("in: " + msg.data)
            q.put(msg, True)

wiki_thread = threading.Thread(target=wiki)
wiki_thread.setDaemon(True)
wiki_thread.start()

raw_input("Enter to stop")
server.shutdown()

sys.exit(0)

