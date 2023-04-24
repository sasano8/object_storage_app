from fastapi import APIRouter


class StorageRouter(APIRouter):
    @classmethod
    def build(cls, *args, **kwargs):
        router = cls(*args, **kwargs)
