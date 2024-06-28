import json
import os
import pytest
import simplest_database


@pytest.fixture
async def filename():
    filename = 'test.db'
    yield filename
    os.remove(filename)


@pytest.fixture
async def db(filename):
    db = simplest_database.SimplestDatabase(filename=filename)
    await db.set('greeting',  {'hello': 'world'})
    
    yield db


async def test_set(db):
    with open(db.filename) as f:
        assert f.read() == 'greeting,{"hello": "world"}\n'


async def test_get(db):
    assert await db.get('greeting') == {"hello": "world"}
