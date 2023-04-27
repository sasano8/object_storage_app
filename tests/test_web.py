import pytest
from httpx import AsyncClient

from metadata_store.app import app
from metadata_store.exeptions import Http409_Conflict
from .conftest import get_file, clear_files
import os


@pytest.fixture
async def client():
    await clear_files()
    async with AsyncClient(app=app, base_url="http://127.0.0.1:8000/v1") as ac:
        yield ac


@pytest.mark.anyio
async def test_inspect(client: AsyncClient):
    response = await client.get("/inspect")
    assert response.status_code == 200
    res = response.json()
    assert res == ["AsyncLocalStorage"]


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

    test_dir = os.path.join(os.path.dirname(__file__), "dir_asgi")
    if os.path.exists(test_dir + "/output1"):
        raise Exception("output1 already exists")

    # ファイルが存在しない場合createできること
    with get_file("1.txt", "rb") as f:
        res = await client.post(
            "/create", files={"file": f}, params={"dest": "output1"}
        )  # query paramじゃなくてjsonにしたい
        assert res.status_code == 200
        val = res.json()
        assert isinstance(val, dict)

    res = await client.post("/load", params={"src": "output1", "mode": "rb"})
    assert res.status_code == 200
    assert res.read() == b"1"  # なんでasyncじゃねーんだよ

    res = await client.get("/ll")
    assert res.status_code == 200
    val = res.json()
    assert len(val) == 1
    assert val[0]["name"] == "output1"

    # # ファイルが存在する場合createできないこと
    with get_file("1.txt", "rb") as f:
        res = await client.post(
            "/create", files={"file": f}, params={"dest": "output1"}
        )  # query paramじゃなくてjsonにしたい
        assert res.status_code == 409  # conflict
        val = res.json()
        assert isinstance(val, dict)

    res = await client.get("/ll")
    assert res.status_code == 200
    val = res.json()
    assert len(val) == 1
    assert val[0]["name"] == "output1"

    # # ファイルを置き換えることができること
    with get_file("2.txt", "rb") as f:
        res = await client.post(
            "/put", files={"file": f}, params={"dest": "output1"}
        )  # query paramじゃなくてjsonにしたい
        assert res.status_code == 200
        val = res.json()
        assert isinstance(val, dict)

    res = await client.post("/load", params={"src": "output1", "mode": "rb"})
    assert res.status_code == 200
    assert res.read() == b"2"  # なんでasyncじゃねーんだよ

    res = await client.get("/ll")
    assert res.status_code == 200
    val = res.json()
    assert len(val) == 1
    assert val[0]["name"] == "output1"

    # ファイルを削除できること
    res = await client.post("/delete", params={"src": "output1"})
    assert res.status_code == 200
    assert res.json() == 1

    res = await client.get("/ll")
    assert res.status_code == 200
    val = res.json()
    assert val == []


# def test_aaa():
#     grpc_client = "aaa"
#     http_client = AsyncClient(app=app, base_url="http://aaaa/v1")
#     a = FSWrapper(http_client)

# import boto3
# from botocore.client import S3

# s3_client = boto3.client("s3", region_name="us-east-1")

# bucket = "somebucket"
# key = "some/prefix/to/an/object"

# print(s3_client.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600))


# 認証・認可

# IAM方式
# APIKEY/SECRETKEY方式
# OICD方式
# presigned_url（APIキーを使用した共通鍵方式）
