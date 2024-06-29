import asyncio
import functools
import json


class _Index:
    def __init__(self):
        self._idx_map = {}
        self._cursor = 0

    async def add_next(self, line: str, key=None):
        if key is None:
            key, _  = line.split(',', 1)
        length = len(line)
        self._idx_map[key] = (self._cursor, length)
        self._cursor += length

    async def get(self, key):
        return self._idx_map.get(key)


def _load_index(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        tasks = []
        if not self._index_loaded:
            try:
                with open(self.filename, 'r') as f:
                    for line in f:
                        tasks.append(asyncio.create_task(self._index.add_next(line)))
            except FileNotFoundError:
                pass
            self._index_loaded = True
            for task in tasks:
                await task

        return await func(self, *args, **kwargs)

    return wrapper


class SimpleDbInMemoryIndex:
    def __init__(self, filename='database'):
        self.filename = filename
        self._index = _Index()
        self._index_loaded = False

    @_load_index
    async def set(self, key, json_dict):
        line = f'{key},{json.dumps(json_dict)}\n'
        task = asyncio.create_task(self._index.add_next(line, key=key))
        with open(self.filename, 'a') as f:
            f.write(line)
        await task

    @_load_index
    async def get(self, key):
        try:
            offset, length = await self._index.get(key) 
        except TypeError:
            return None

        with open(self.filename, 'r') as f:
            f.seek(offset)
            return json.loads(f.read(length).split(',', 1)[1].strip())