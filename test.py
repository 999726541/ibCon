import threading
import multiprocessing
from time import sleep


def worker(tt):
    print(threading.currentThread().getName(), 'Starting')
    sleep(50)
    print(threading.currentThread().getName(), 'Ending')


cpus=multiprocessing.cpu_count() #detect number of cores

print("Creating %d threads" % cpus)
all_t = []
all_t_name = []

while True:
    q = open('/Users/leotao/PycharmProjects/ibCon/123').read()
    q = list(q)
    while len(q) > 0:
        i = q.pop()
        if i not in all_t_name:
            t = threading.Thread(name=i,target=worker,args=([i]))
            all_t.append(t)
            all_t_name.append(i)
            t.daemon = True
            t.start()
    # for ttt in all_t:
        # print(ttt.is_alive())

    sleep(1)