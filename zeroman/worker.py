import os
import time
import zmq
import random

import logging
logger = logging.getLogger(__name__)

TIMEOUT = 5000
SERVER_DEAD_TIME = 5

class worker:
    def __init__(self, servers, timeout=TIMEOUT, server_dead_time=SERVER_DEAD_TIME):
        self.context = zmq.Context()
        self.sockets = {}
        self.dead = set()
        self.last_heartbeats={}
        self.functions = {}

        self.servers = servers
        self.timeout = timeout
        self.server_dead_time = server_dead_time


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
            logger.debug('got reply from %r', h)
        else:
            logger.error('no reply from %r', h)
            reply = None
            s.setsockopt(zmq.LINGER, 0)
            s.close()
            del self.sockets[h]
            self.dead[h] = time.time()
        poll.unregister(s)

        return reply

    def register(self, h):
        s = self.get_socket(h)
        s.send_multipart(["register"] + self.functions.keys())

    def close(self, h):
        s = self.get_socket(h)
        s.setsockopt(zmq.LINGER, 0)
        s.close()
        del self.sockets[h]

    def reconnect_if_needed(self):
        for h, s in self.sockets.items():
            if time.time() -  self.last_heartbeats.get(s, 0) > self.server_dead_time:
                if h not in self.dead:
                    logger.error("%r is dead", h)
                    self.dead.add(h)
                    self.close(h)
                    self.register(h)
            elif h in self.dead:
                logger.info("%r is alive", h)
                self.dead.remove(h)
                    
        

    def handle(self, s, msg):
        command = msg.pop(0)
        func = getattr(self, "handle_%s" % command)
        if func:
            return func(s, *msg)
        else:
            return "eh?"

    def handle_heartbeat(self, s):
        logger.debug("heartbeat from %r", s)
        self.last_heartbeats[s] = time.time()
        s.send_multipart(["alive"])

    def handle_call(self, s, client, func, msg):
        f = self.functions.get(func)
        ret = f(msg)
        s.send_multipart(['ret', client, func, ret])

    def handle_do(self, s, client, func, msg):
        f = self.functions.get(func)
        f(msg)
        s.send_multipart(["worker_ready", func])

    def handle_bc(self, s, client, func, msg):
        f = self.functions.get(func)
        f(msg)
        #FIXME: make it so this is not needed
        s.send_multipart(["alive"])

    def run(self):
        for h in self.servers:
            self.register(h)
        
        time.sleep(1)
        while True:
            poll = zmq.Poller()
            for s in self.sockets.values():
                poll.register(s, zmq.POLLIN)
            socks = dict(poll.poll(self.timeout))
            for s in socks:
                msg = s.recv_multipart()
                self.handle(s, msg)

            self.reconnect_if_needed()
            
    def register_handler(self, name, func):
        self.functions[name] = func
