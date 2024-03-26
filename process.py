from multiprocessing import Process
import time 
import os
import math 
start_time=time.time()

def calc():
    for i in range(0,100000):
        print(math.sqrt(i))

processes =[]

for i in range(os.cpu_count()):
    print('registering process %d'%i)
    processes.append(Process(target=calc))

if __name__ == '__main__':
                
    for process in processes:
        process.start()

    for process in processes:
        process.join()

print("--- %s seconds ---" % (time.time() - start_time))

