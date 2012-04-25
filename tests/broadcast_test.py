import time
from zeroman import client
c=client(['tcp://localhost:1234','tcp://localhost:1235'])
for x in range(1000):
    c.broadcast("hello", "broadcast %d" % x)
    time.sleep(.01)
