import json
import random
import requests
import time

if __name__ == '__main__':

    i = 0

    while True:

        t = time.time()

        item = {}
        item['t'] = t
        item['k'] = 'test' + str(i)
        item['v'] = int(t % 100)

        s = random.randrange(10, 100) / 10

        print(json.dumps(item) + ' -> ' + str(s))

        requests.post('http://localhost:8080/in', json=item)

        time.sleep(s)

        i = i+1