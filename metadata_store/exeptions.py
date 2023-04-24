from starlette.exceptions import HTTPException


class HTTPExceptionBase(HTTPException):
    def __init__(self, detail: str = None, headers: dict = None):
        code = self.STATUS_CODE
        super().__init__(code, detail, headers)


class Http500InternalError(HTTPExceptionBase):
    STATUS_CODE = 500


class Http405_Method_Not_Allowed(HTTPExceptionBase):
    STATUS_CODE = 405


class Http409_Conflict(HTTPExceptionBase):
    STATUS_CODE = 409


class Http411LengthRequired(HTTPExceptionBase):
    STATUS_CODE = 411


class Http400BadRequest(HTTPExceptionBase):
    STATUS_CODE = 411
