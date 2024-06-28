import json


class SimplestDatabase:
    def __init__(self, filename='database'):
        self.filename = filename

    async def set(self, key, json_dict):
        with open(self.filename, 'a') as f:
            f.write(f'{key},{json.dumps(json_dict)}\n')


    async def get(self, key):
        matching_rows = []
        with open(self.filename, 'r') as f:
            for line in f:
                k, v = line.strip().split(',')
                if k == key:
                    matching_rows.append(v)

        return json.loads(matching_rows[-1]) if matching_rows else None