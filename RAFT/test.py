import signal
import time
signal.signal(signal.SIGALRM, lambda x,y: print('Alarm!',time.time()))
print('Starting alarm...',time.time())
signal.setitimer(signal.ITIMER_REAL,5,5)
time.sleep(3)
print('Starting alarm again...',time.time())
signal.setitimer(signal.ITIMER_REAL,3,3)
while True:
    pass
