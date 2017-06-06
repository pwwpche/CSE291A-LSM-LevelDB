
class InMemTable():
    def __init__(self):
        self.mem = {}

    def get(self, key):
        return self.mem.get(key, None)

    def put(self, key, value):
        self.mem[key] = value

    def remove(self, key):
        del self.mem[key]
