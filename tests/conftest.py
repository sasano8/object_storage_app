import pytest
import os
import shutil
import time
import asyncer
import asyncio
import anyio


@pytest.fixture
def anyio_backend():
    """event loop backend は asyncio のみ対応。trioは対応しない。"""
    return "asyncio"


def get_file(src: str, mode: str = "r"):
    target = os.path.join(os.path.dirname(__file__), "samples", src)
    return open(target, mode)


# @pytest.fixture(scope="module", autouse=True)
# @pytest.fixture(scope="function", autouse=True)
async def clear_files():
    test_dir = os.path.join(os.path.dirname(__file__), "dir_asgi")

    for item in os.listdir(test_dir):
        target = os.path.join(test_dir, item)
        if os.path.isfile(target):
            print(f"file {target}")
            await asyncer.asyncify(os.unlink)(target)
        elif os.path.isdir(target):
            print(f"dir {target}")
            await asyncer.asyncify(shutil.rmtree)(target)

        while True:
            if await asyncer.asyncify(os.path.exists)(target):
                ...
            else:
                break
