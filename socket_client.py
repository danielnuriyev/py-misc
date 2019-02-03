import socket

HOST, PORT = "52.202.156.203", 9000
#HOST, PORT = "127.0.0.1", 9006

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

buffer = ''
while True:

    data = sock.recv(1024)
    buffer = buffer + data

    if len(data) and data[-1] == '\n':
        print buffer
        buffer = ''

    #else:
    #    sock.close()
    #    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #    sock.connect((HOST, PORT))
