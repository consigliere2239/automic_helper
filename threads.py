from threading import Thread
import time
import os 
import math
start_time =time.time()

def calc():
    for i in range (0,100000):
        print(math.sqrt(i))

threads=[]

for i in range (os.cpu_count()):
    print('registering thread %d' %i)
    threads.append(Thread(target=calc))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()


print("--- %s seconds ---" % (time.time() - start_time))