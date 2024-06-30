import os
import pytest
import shutil
from pathlib import Path

import simple_db_multi_segments


@pytest.fixture
async def dbname():
    dbname = 'testdb'
    yield dbname
    try:
        shutil.rmtree(dbname)
    except NotADirectoryError:
        os.remove(dbname)


@pytest.fixture
async def db(dbname):
    db = simple_db_multi_segments.SimpleDbMultiSegments(dbname=dbname, segment_bytes_threshold=50)
    await db.set('greeting',  {'hello': 'world'})
    await db.set('micu',  {'species': 'cat', 'color': 'black', 'age': 3})
    await db.set('menu',  {
        'breakfast': 'bubur ayam', 'lunch': 'nasi rendang', 'dinner': 'nasi goreng'
    })
    yield db


async def test_existing_db(db):
    db2 = simple_db_multi_segments.SimpleDbMultiSegments(dbname=db.dbname)

    assert await db2.get('menu') == {"breakfast": "bubur ayam", "lunch": "nasi rendang", "dinner": "nasi goreng"}
    assert await db2.get('greeting') == {"hello": "world"}
    assert await db2.get('micu') == {"species": "cat", "color": "black", "age": 3}

    assert len(db2._indexes) == 2
    assert db2._indexes[0]._segment_name != db2._indexes[1]._segment_name
    assert "greeting" in db2._indexes[0]._idx_map
    assert "greeting" not in db2._indexes[1]._idx_map
    assert "micu" in db2._indexes[0]._idx_map
    assert "micu" not in db2._indexes[1]._idx_map
    assert "menu" not in db2._indexes[0]._idx_map
    assert "menu" in db2._indexes[1]._idx_map


async def test_set(db):
    with open(db._indexes[0]._segment_name) as f:
        assert f.readline() == 'greeting,{"hello": "world"}\n'
        assert f.readline() == 'micu,{"species": "cat", "color": "black", "age": 3}\n'

    with open(db._indexes[1]._segment_name) as f:
        assert f.readline() == 'menu,{"breakfast": "bubur ayam", "lunch": "nasi rendang", "dinner": "nasi goreng"}\n'


async def test_get(db):
    assert await db.get('menu') == {"breakfast": "bubur ayam", "lunch": "nasi rendang", "dinner": "nasi goreng"}
    assert await db.get('greeting') == {"hello": "world"}
    assert await db.get('micu') == {"species": "cat", "color": "black", "age": 3}


async def test_get_invalid_key(db):
    assert await db.get('invalid key') == None


async def test_invalid_db_folder(db):
    directory = Path(db.dbname)
    marker = directory / '.simple_db_multi_segments_marker'
    os.remove(marker)
    db2 = simple_db_multi_segments.SimpleDbMultiSegments(dbname=db.dbname)
    with pytest.raises(simple_db_multi_segments.IsADirectoryError):
        await db2.set('greeting',  {'hello': 'world'})


async def test_b_name_conflict(db):
    shutil.rmtree(db.dbname)
    Path.touch(Path(db.dbname))
    db2 = simple_db_multi_segments.SimpleDbMultiSegments(dbname=db.dbname)
    with pytest.raises(simple_db_multi_segments.IsADirectoryError):
        await db2.set('greeting',  {'hello': 'world'})