from zeroman import worker

def hello(data):
    print 'returning', data.upper()
    return data.upper()

w = worker(['tcp://localhost:1234','tcp://localhost:1235'])
w.register_handler('hello', hello)
w.run()
