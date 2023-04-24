from fastapi import FastAPI
from .api import router


app = FastAPI(swagger_ui_parameters={"tryItOutEnabled": True})
app.include_router(router, prefix="/v1")


def liveness():
    return {"result": "alive"}


def readiness():
    return {"result": "ready"}
