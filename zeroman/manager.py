import os
import time
import zmq
import random
from collections import defaultdict
import logging
logger = logging.getLogger(__name__)

class worker:
    def __init__(self, id, handlers):
        self.id = id
        self.handlers = set(handlers)
        self.last_checkin = time.time()
        self.last_heartbeat = 0

class manager:
    HEARTBEAT_INTERVAL = 2
    def __init__(self, port):
        context = zmq.Context()
        s = context.socket(zmq.ROUTER)
        s.bind("tcp://*:%s" % port)
        self.s = s
        self.workers = []
        self.workers_by_id = {}
        self.workers_by_handler = defaultdict(list)
        self.work_queue = defaultdict(list)

    def handle_command(self, command, id, msg):
        func = getattr(self, "handle_%s" % command)
        if func:
            return func(id, *msg)
        return "eh?"

    def handle_register(self, id, *handlers):
        w = worker(id, handlers)
        self.workers.append(w)
        self.workers_by_id[id] = w
        for h in handlers:
            self.workers_by_handler[h].append(w)
        logger.info("%r is a worker %s", w, handlers)
        self.send_heartbeat(w)

        for h in handlers:
            if self.work_queue[h]:
                client_id, type, data = self.work_queue[h].pop(0)
                self.s.send_multipart([id, '', type, client_id, h, data])


    def handle_alive(self, id, *args):
        w = self.workers_by_id[id]
        w.last_checkin = time.time()
        logger.debug("%r is alive", w)

    def handle_call(self, id, func, data):
        workers = self.workers_by_handler[func]
        if workers:
            worker = workers.pop(0)
            self.s.send_multipart([worker.id, '', 'call', id, func, data])
        else:
            self.work_queue[func].append((id, 'call', data))

    def handle_background(self, id, func, data):
        workers = self.workers_by_handler[func]
        if workers:
            worker = workers.pop(0)
            self.s.send_multipart([worker.id, '', 'do', id, func, data])
        else:
            self.work_queue[func].append((id, 'do', data))
        self.s.send_multipart([id, '', "ok"])

    def handle_broadcast(self, id, func, data):
        workers = self.workers_by_handler[func]
        for w in workers:
            self.s.send_multipart([w.id, '', 'bc', id, func, data])
        self.s.send_multipart([id, '', "ok"])


    def handle_ret(self, id, client, func, response):
        self.s.send_multipart([client, '', response])
        self.handle_worker_ready(id, func)

    def handle_worker_ready(self, id, func):
        w = self.workers_by_id[id]
        resent = False
        for f in w.handlers:
            if self.work_queue[f]:
                client_id, type, data = self.work_queue[f].pop(0)
                self.s.send_multipart([id, '', type, client_id, f, data])
                if f == func:
                    resent = True
        if not resent:
            self.workers_by_handler[func].append(w)
            

    def send_heartbeat(self, w):
        self.s.send_multipart([w.id, '', 'heartbeat'])
        w.last_heartbeat = time.time()

    def send_heartbeats(self):
        for w in self.workers:
            if time.time() - w.last_heartbeat > self.HEARTBEAT_INTERVAL:
                self.send_heartbeat(w)

    def check_for_dead(self):
        for w in self.workers:
            if time.time() - w.last_checkin > self.HEARTBEAT_INTERVAL * 2:
                logger.error("%r is dead", w)
                self.cleanup(w)

    def cleanup(self, w):
        del self.workers_by_id[w.id]
        self.workers.remove(w)
        for handler, workers in self.workers_by_handler.items():
            if w in workers:
                workers.remove(w)

    def run(self):
        while True:
            poll = zmq.Poller()
            poll.register(self.s, zmq.POLLIN)
            ret = poll.poll(1000)
            if ret:
                m = self.s.recv_multipart()
                id = m.pop(0)
                _ = m.pop(0)
                command = m.pop(0)
                self.handle_command(command, id, m)

            self.send_heartbeats()
            self.check_for_dead()

def main():
    import sys
    port = sys.argv[1]
    manager(port).run()
