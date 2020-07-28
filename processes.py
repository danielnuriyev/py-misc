from multiprocessing import Process
import time

def f():
    time.sleep(2) # doing something
    print("sub")

if __name__ == '__main__':

    p = Process(target=f)
    p.start()
    # p.join()

    print("main")
