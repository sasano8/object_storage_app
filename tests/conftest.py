import pytest
import os
import shutil


def get_file(src: str, mode: str = "r"):
    target = os.path.join(os.path.dirname(__file__), "samples", src)
    return open(target, mode)


@pytest.fixture(scope="module", autouse=True)
def clear_files():
    test_dir = os.path.join(os.path.dirname(__file__), "dir_asgi")

    for item in os.listdir(test_dir):
        target = os.path.join(test_dir, item)
        if os.path.isfile(target):
            os.remove(target)
        elif os.path.isdir(target):
            shutil.rmtree(target)
