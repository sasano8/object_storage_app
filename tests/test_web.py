import pytest
from httpx import AsyncClient

from metadata_store.app import app
from .conftest import get_file


@pytest.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://127.0.0.1:8000/v1") as ac:
        yield ac


@pytest.mark.anyio
async def test_inspect(client: AsyncClient):
    response = await client.get("/inspect")
    assert response.status_code == 200
    res = response.json()
    assert res == ["LocalStorage"]


@pytest.mark.anyio
async def test_scenario(client: AsyncClient):
    res = await client.get("/ll")
    assert res.status_code == 200
    val = res.json()
    assert val == []

    res = await client.get("/exists", params={})
    assert res.status_code == 200
    val = res.json()
    assert val

    res = await client.get("/exists", params={"src": "output1"})
    assert res.status_code == 200
    val = res.json()
    assert val == False

    with get_file("1.txt", "rb") as f:
        res = await client.post(
            "/create", files={"file": f}, params={"dest": "output1"}
        )  # query paramじゃなくてjsonにしたい
        assert res.status_code == 200
        val = res.json()
        assert val == False

    # # ファイルが存在しない場合createできること
    # with get_temp_file(b"1", "rb+") as input:
    #     storage.create(input, "output1")

    # with storage.open("output1", "rb") as f:
    #     assert f.read() == b"1"

    # assert len(list(storage.ll(""))) == 1
    # assert list(storage.ll(""))[0]["name"] == "output1"

    # # ファイルが存在する場合createできないこと
    # with get_temp_file(b"2", "rb+") as input:
    #     with pytest.raises(Http409_Conflict):
    #         storage.create(input, "output1")

    # assert len(list(storage.ll(""))) == 1
    # assert list(storage.ll(""))[0]["name"] == "output1"

    # # ファイルを置き換えることができること
    # with get_temp_file(b"2", "rb+") as input:
    #     storage.put(input, "output1")

    # with storage.open("output1", "rb") as f:
    #     assert f.read() == b"2"

    # assert len(list(storage.ll(""))) == 1
    # assert list(storage.ll(""))[0]["name"] == "output1"

    # # ファイルを削除できること
    # storage.delete("output1")
    # assert len(list(storage.ll(""))) == 0
    # assert list(storage.ll("")) == []
