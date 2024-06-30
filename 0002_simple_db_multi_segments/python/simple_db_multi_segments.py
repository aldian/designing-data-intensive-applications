import asyncio
import functools
import json
import time
from pathlib import Path


class IsADirectoryError(Exception):
    pass


class _Index:
    def __init__(self, segment_name):
        self._segment_name = segment_name
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



# A segment file has a name pattern of 'segment_{unix epoch microsecond timestamp}.db'
_SEGMENT_FILE_PATTERN = "segment_*.db"


def _check_db_directory(dbname):
    directory = Path(dbname)
    marker = directory / '.simple_db_multi_segments_marker'

    if directory.exists():
        if directory.is_dir():
            if not marker.exists():
                raise IsADirectoryError
        else:
            raise IsADirectoryError
    else:
        directory.mkdir()
        marker.touch()

    return directory


async def _load_index(index):
    with open(index._segment_name, 'r') as f:
        tasks = []
        for line in f:
            tasks.append(asyncio.create_task(index.add_next(line)))
        for task in tasks:
            await task


def _load_indexes(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._indexes_loaded:
            directory = _check_db_directory(self.dbname)    

            tasks = []
            matching_files = sorted(directory.glob(_SEGMENT_FILE_PATTERN))
            for file_path in matching_files:
                index = _Index(file_path)
                self._indexes.append(index)
                tasks.append(asyncio.create_task(_load_index(index)))
            for task in tasks:
                await task
            self._indexes_loaded = True

        return await func(self, *args, **kwargs)

    return wrapper


class SimpleDbMultiSegments:
    def __init__(self, dbname='database', segment_bytes_threshold=1024 * 1024):
        self.dbname = dbname
        self._indexes = []
        self._indexes_loaded = False
        self._segment_bytes_threshold = max(segment_bytes_threshold, 1)  # Ensure segment_bytes_threshold is at least 1

    @_load_indexes
    async def set(self, key, json_dict):
        if len(self._indexes) < 1 or self._indexes[-1]._cursor >= self._segment_bytes_threshold:
            index = _Index(f'{self.dbname}/segment_{int(time.time() * 1_000_000)}.db')
            self._indexes.append(index)
        else:
            index = self._indexes[-1]

        line = f'{key},{json.dumps(json_dict)}\n'
        task = asyncio.create_task(index.add_next(line, key=key))
        with open(index._segment_name, 'a') as f:
            f.write(line)
        await task

    @_load_indexes
    async def get(self, key):
        for index in reversed(self._indexes):
            try:
                offset, length = await index.get(key) 
                break
            except TypeError:
                continue
        else:
            return None

        with open(index._segment_name, 'r') as f:
            f.seek(offset)
            return json.loads(f.read(length).split(',', 1)[1].strip())

    @_load_indexes
    async def compact(self, new_segment_bytes_threshold=None):
        if new_segment_bytes_threshold is None:
            segment_bytes_threshold = self._segment_bytes_threshold
        else:
            segment_bytes_threshold = new_segment_bytes_threshold

        checked_keys = set()
        new_indexes = []
        new_index = _Index(f'{self.dbname}/segment_{int(time.time() * 1_000_000)}.db')
        for index in reversed(self._indexes):
            for key in index._idx_map:
                if key in checked_keys:
                    continue
                checked_keys.add(key)

                if new_index._cursor >= segment_bytes_threshold:
                    new_indexes.append(new_index)
                    new_index = _Index(f'{self.dbname}/segment_{int(time.time() * 1_000_000)}.db')

                value = await self.get(key)
                line = f'{key},{json.dumps(value)}\n'
                await new_index.add_next(line, key=key)

                with open(new_index._segment_name, 'a') as f:
                    f.write(line)

        if new_index._idx_map:
            new_indexes.append(new_index)

        self._indexes = new_indexes