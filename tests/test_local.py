import os
import shutil
import tempfile
from contextlib import contextmanager

import pytest
from metadata_store.store import LocalStorage, VirtualStorage
from metadata_store.exeptions import Http409_Conflict
from metadata_store.app import app


import pytest


def get_test_dir(path):
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), path)
    return path


@pytest.fixture
def persistent():
    root = get_test_dir("dir_local_storage")
    os.makedirs(root, exist_ok=True)

    for item in os.listdir(root):
        path = os.path.join(root, item)
        if os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            raise Exception()

    return root


@contextmanager
def get_temp_file(val=None, mode="w"):
    if val:
        with tempfile.NamedTemporaryFile(mode) as f:
            f.write(val)
            f.flush()
            f.seek(0)
            yield f
    else:
        with tempfile.NamedTemporaryFile(mode) as f:
            yield f


def test_local_path_join(persistent: str):
    storage = LocalStorage(persistent)
    assert storage._join("a") == os.path.join(persistent, "a")


def test_local_storage_base(persistent: str):
    storage = LocalStorage(persistent)
    assert list(storage.ll("")) == []
    assert storage.exists("") == True
    assert isinstance(storage.info(""), dict)


def test_local_storage_scenario(persistent: str):
    storage = LocalStorage(persistent)
    assert len(list(storage.ll(""))) == 0
    assert storage.exists("output1") == False

    # ファイルが存在しない場合createできること
    with get_temp_file(b"1", "rb+") as input:
        storage.create(input, "output1")

    with storage.open("output1", "rb") as f:
        assert f.read() == b"1"

    assert len(list(storage.ll(""))) == 1
    assert list(storage.ll(""))[0]["name"] == "output1"

    # ファイルが存在する場合createできないこと
    with get_temp_file(b"2", "rb+") as input:
        with pytest.raises(Http409_Conflict):
            storage.create(input, "output1")

    assert len(list(storage.ll(""))) == 1
    assert list(storage.ll(""))[0]["name"] == "output1"

    # ファイルを置き換えることができること
    with get_temp_file(b"2", "rb+") as input:
        storage.put(input, "output1")

    with storage.open("output1", "rb") as f:
        assert f.read() == b"2"

    assert len(list(storage.ll(""))) == 1
    assert list(storage.ll(""))[0]["name"] == "output1"

    # ファイルを削除できること
    storage.delete("output1")
    assert len(list(storage.ll(""))) == 0
    assert list(storage.ll("")) == []


def test_virtual_buckets(persistent: str):
    storage = LocalStorage(persistent)
    vstorage = VirtualStorage(bucket1=storage, bucket2=storage)

    assert list(vstorage.ll("")) == [{"name": "bucket1"}, {"name": "bucket2"}]
    assert list(vstorage.ll("bucket1/")) == []
