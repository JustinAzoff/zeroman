from zeroman import client
c=client(['tcp://localhost:1234'])
for x in range(1000):
    print c.call("hello", "world %d" % x)
