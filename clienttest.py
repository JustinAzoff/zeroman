import os
import time
import zmq
import random


class client:
    hosts = ["tcp://localhost:5555", "tcp://localhost:5556"]
    TIMEOUT = 1000
    HOST_DEAD_TIME = 5

    def __init__(self):
        self.context = zmq.Context()
        self.sockets = {}
        self.dead = {}

    def get_socket(self, h):
        s = self.sockets.get(h)
        if s:
            return s
        s = self.context.socket(zmq.REQ)
        s.connect(h)
        self.sockets[h] = s
        return s

    def do_host(self, h, r):
        s = self.get_socket(h)
        if not s:
            return None
        s.send_multipart(r)

        poll = zmq.Poller()
        poll.register(s, zmq.POLLIN)
        socks = dict(poll.poll(self.TIMEOUT))
        if socks.get(s) == zmq.POLLIN:
            reply = s.recv()
            print 'got reply from', h
        else:
            print 'no reply from', h
            reply = None
            s.setsockopt(zmq.LINGER, 0)
            s.close()
            del self.sockets[h]
            self.dead[h] = time.time()
        poll.unregister(s)

        return reply

    def alive_hosts(self):
        for h in self.hosts:
            if h in self.dead:
                if time.time() - self.dead[h] > self.HOST_DEAD_TIME:
                    del self.dead[h]
                    yield h
            else:
                yield h

    def do_req(self, r):
        random.shuffle(self.hosts)
        if len(self.dead) == len(self.hosts):
            self.dead = {}
        for h in self.alive_hosts():
            resp = self.do_host(h, r)
            if resp:
                return resp

    def call(self, func, data):
        return self.do_req(["call", func, data])

    def run(self):
        for x in range(1000):
            print self.call("hello", "world %d" % x)

if __name__ == "__main__":
    client().run()
