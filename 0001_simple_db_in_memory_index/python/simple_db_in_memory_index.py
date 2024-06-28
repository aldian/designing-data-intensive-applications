import json


class _Index:
    def __init__(self):
        self._idx_map = {}
        self._cursor = 0

    def add_next(self, line: str, key=None):
        if key is None:
            key, _  = line.split(',', 1)
        length = len(line)
        self._idx_map[key] = (self._cursor, length)
        self._cursor += length

    def get(self, key):
        return self._idx_map.get(key)

class SimpleDbInMemoryIndex:
    def __init__(self, filename='database'):
        self.filename = filename
        self._index = _Index()

        try:
            with open(self.filename, 'r') as f:
                for line in f:
                    self._index.add_next(line)
        except FileNotFoundError:
            pass

    async def set(self, key, json_dict):
        line = f'{key},{json.dumps(json_dict)}\n'
        with open(self.filename, 'a') as f:
            f.write(line)
        self._index.add_next(line, key=key)

    async def get(self, key):
        try:
            offset, length = self._index.get(key) 
        except TypeError:
            return None

        with open(self.filename, 'r') as f:
            f.seek(offset)
            return json.loads(f.read(length).split(',', 1)[1].strip())