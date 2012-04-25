import time
from zeroman import client
c=client(['tcp://localhost:1234','tcp://localhost:1235'])
import time
s=time.time()
for x in xrange(1,10000):
    c.background("hello", "world %d" % x)
    if x %1000 ==0:
        e = time.time()
        print "1000 in %0.2f seconds %0.2f/sec" % (e-s, 1000.0/(e-s))
        s = e

