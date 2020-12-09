from multiprocessing import Process
from threading import Thread
import time

def f():
    time.sleep(2) # doing something
    print("sub")

if __name__ == '__main__':
    p = Process(target=f)
    # p = Thread(target=f)
    p.start()
    p.join()

    print("main")