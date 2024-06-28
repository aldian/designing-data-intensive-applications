import os
import pytest
import simple_db_in_memory_index


@pytest.fixture
async def filename():
    filename = 'test.db'
    yield filename
    os.remove(filename)


@pytest.fixture
async def db(filename):
    db = simple_db_in_memory_index.SimpleDbInMemoryIndex(filename=filename)
    await db.set('greeting',  {'hello': 'world'})
    await db.set('menu',  {
        'breakfast': 'bubur ayam', 'lunch': 'nasi rendang', 'dinner': 'nasi goreng'
    })
    yield db


async def test_existing_db(db):
    db2 = simple_db_in_memory_index.SimpleDbInMemoryIndex(filename=db.filename)
    assert db2._index._idx_map == {'greeting': (0, 28), 'menu': (28, 83)}


async def test_set(db):
    with open(db.filename) as f:
        assert f.readline() == 'greeting,{"hello": "world"}\n'
        assert f.readline() == 'menu,{"breakfast": "bubur ayam", "lunch": "nasi rendang", "dinner": "nasi goreng"}\n'


async def test_get(db):
    assert await db.get('menu') == {"breakfast": "bubur ayam", "lunch": "nasi rendang", "dinner": "nasi goreng"}
    assert await db.get('greeting') == {"hello": "world"}


async def test_get_invalid_key(db):
    assert await db.get('invalid key') == None