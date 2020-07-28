import threading
import time

def f(max):

    start = time.time()

    sum = 0
    for i in range(max):
        sum += i

    print(str(time.time() - start))

a = threading.Thread(target=f, args=(10000000,))
a.start()

b = threading.Thread(target=f, args=(10000000,))
b.start()