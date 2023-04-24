from genericpath import isdir
import os
from posixpath import isabs
import shutil
import tempfile

from .exeptions import Http409_Conflict, Http500InternalError

# ストアとは

"""
- root_directory
    - db.sqlite3
    - artifacts
        - xxx1
        - xxx2
        - xxx3
"""

from sqlmodel import SQLModel, Field
from contextlib import contextmanager


class Tenant(SQLModel, table=True):
    id: str = Field(primary_key=True)
    tenant_name: str
    description: str = ""
    created_at: str
    updated_at: str

    def create(self, db):
        ...

    def delete(self, db):
        ...

    def update(self, db):
        ...


class Experiment(SQLModel, table=True):
    """ユーザーが自由にまとめたい単位"""

    id: str = Field(primary_key=True)
    tenant_id: str
    name: str
    description: str = ""
    created_at: str
    updated_at: str


class Run(SQLModel, table=True):
    """一回の実行で一つのアーティファクトが生成される"""

    id: str = Field(primary_key=True)
    tenant_id: str
    experiment_id: str
    name: str
    description: str = ""
    created_at: str
    updated_at: str
    started_at: str
    ended_at: str

    def to_storage(self):
        ...


class Tags(SQLModel, table=True):
    """検索できればいいので任意のタイミングでインデクシングできるようにする"""

    id: str = Field(primary_key=True)
    tenant_id: str
    experiment_id: str
    run_id: str
    tag: str


def config_spec():
    return {
        "db": ["sqlite", "postgresql"],
        "artifact": ["local", "s3"],
        "index": ["elasticsearch", "pandas"],
    }


def migration(_from, to):
    ...


def push(local, upstream):
    ...


def creation_date(path_to_file):
    # st_birthtime は linux で使えない
    if platform.system() == "Windows":
        return os.path.getctime(path_to_file)
    else:
        stat = os.stat(path_to_file)
        try:
            # return stat.st_birthtime
            return datetime.fromtimestamp(stat.st_birthtime, tz=timezone.utc)
        except AttributeError:
            # return stat.st_mtime
            return datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)


def stat_to_dict(
    name: str,
    stat,
    reflection={
        "st_size": "st_size",
        "st_atime": "st_atime",
        "st_mtime": "st_mtime",
        "st_ctime": "st_ctime",
        "st_birthtime": "st_birthtime",
    },
):
    obj = {"name": name}
    for src, dest in reflection.items():
        obj[dest] = getattr(stat, src, None)
    return obj


class LocalFileDownloader:
    def __init__(self, src):
        ...


class AbstractStorage:
    def __iter_storage__(self):
        yield self.__class__.__name__
        if hasattr(self, "storage"):
            yield from self.storage.__iter_storage__()

    def get_storage_pipe(self):
        return list(self.__iter_storage__())

    def ll(self, src):
        raise NotImplementedError()

    def info(self, src):
        raise NotImplementedError()

    def exists(self, src: str):
        raise NotImplementedError()

    def save(self, file, dest, emit: bool = True):
        """saveとcreate, putの違いは？"""
        raise NotImplementedError()

    def create(self, file, dest, emit: bool = True):
        raise NotImplementedError()

    def put(self, file, dest: str, emit: bool = True):
        raise NotImplementedError()

    def open(self, src, mode: str = "r"):
        raise NotImplementedError()

    def delete(self, src):
        raise NotImplementedError()


class VirtualStorage(AbstractStorage):
    def __init__(self, **storages: AbstractStorage):
        self.storages = storages

    def _split(self, src: str):
        args = [x for x in src.split("/") if x]
        return args

    def ll(self, src: str):
        args = self._split(src)
        if len(args) == 0:
            for bucket_name, storage in self.storages.items():
                yield {"name": bucket_name}
                # for stat in storage.ll("/".join(args[1:])):
                #     stat["name"] = bucket_name + "/" + stat["name"]
                #     yield stat
        else:
            bucket_name = args[0]
            for stat in self.storages[bucket_name].ll("/".join(args[1:])):
                stat["name"] = bucket_name + "/" + stat["name"]
                yield stat

    def info(self, src):
        raise NotImplementedError()

    def exists(self, src: str):
        raise NotImplementedError()

    def save(self, file, dest, emit: bool = True):
        """saveとcreate, putの違いは？"""
        raise NotImplementedError()

    def create(self, file, dest, emit: bool = True):
        raise NotImplementedError()

    def put(self, file, dest: str, emit: bool = True):
        raise NotImplementedError()

    def open(self, src, mode: str = "r"):
        raise NotImplementedError()

    def delete(self, src):
        raise NotImplementedError()


class LocalStorage(AbstractStorage):
    def __init__(self, root: str):
        _ = os.path.abspath(root)
        _ = os.path.normpath(_)

        if not os.path.exists(_):
            raise Exception()

        self.root = _

    def _join(self, path):
        if not path:
            return self.root

        if os.path.isabs(path):
            raise Exception()
        _ = os.path.join(self.root, path)
        return _

    def ll(self, src: str = ""):
        _path = self._join(src)
        for item in os.listdir(_path):
            child = os.path.join(_path, item)
            _stat = os.stat(child, follow_symlinks=False)
            rel_path = os.path.relpath(child, self.root)
            info = stat_to_dict(rel_path, _stat)
            yield info

    def info(self, src: str = ""):
        _path = self._join(src)
        _stat = os.stat(_path, follow_symlinks=False)
        rel_path = os.path.relpath(_path, self.root)
        info = stat_to_dict(rel_path, _stat)
        # os.stat_result(st_mode=16877, st_ino=421427, st_dev=2080, st_nlink=5, st_uid=1000, st_gid=1000, st_size=4096, st_atime=1682185290, st_mtime=1682185286, st_ctime=1682185286)
        # st_birthtime を持っていることがある
        #
        return info

    def exists(self, src: str = ""):
        _path = self._join(src)
        return os.path.exists(_path)

    def create(self, file, dest: str, emit: bool = True):
        if self.exists(dest):
            raise Http409_Conflict()
        _path = self._join(dest)
        _parent = os.path.dirname(_path)
        os.makedirs(_parent, exist_ok=True)
        with open(_path, "wb") as f:
            shutil.copyfileobj(file, f)
            f.flush()

        return self.info(dest)

    def put(self, file, dest: str, emit: bool = True):
        _path = self._join(dest)
        _parent = os.path.dirname(_path)
        os.makedirs(_parent, exist_ok=True)

        if os.path.isdir(_path):
            raise Http409_Conflict("ディレクトリにオブジェクトはputできません")

        with tempfile.NamedTemporaryFile("wb", delete=False) as temp_file:
            try:
                shutil.copyfileobj(file, temp_file)
                temp_file.flush()
                os.fsync(temp_file.fileno())  # ハードディスクに書き込まれたことを保証する
            except Exception as e:
                try:
                    os.unlink(temp_file.name)
                except Exception:
                    ...

                raise

            os.replace(temp_file.name, _path)
        return self.info(dest)

    def open(self, src, mode: str = "r"):
        _path = self._join(src)
        return open(_path, mode)

    def delete(self, src) -> int:
        if self.exists(src):
            _path = self._join(src)
            os.remove(_path)
            return 1
        else:
            return 0


class CompressionStorage(AbstractStorage):
    def __init__(self, storage: AbstractStorage):
        self.storage = storage


class EncryptedStorage(AbstractStorage):
    def __init__(self, storage: AbstractStorage):
        self.storage = storage


class S3Storage(AbstractStorage):
    ...


class PostgresqlStorage(AbstractStorage):
    ...


class Resttorage(AbstractStorage):
    ...


class WebSocketStorage(AbstractStorage):
    ...


class HtmlStorage(AbstractStorage):
    ...


class UserSideStorage(AbstractStorage):
    def __init__(self, local: AbstractStorage, upstream: AbstractStorage):
        self.local = local
        self.upstream = upstream

    def info(self, src):
        self.sync(src)
        return self.local.info(src)

    def exists(self, src):
        self.sync(src)
        return self.local.exists(src)

    @contextmanager
    def open(self, src, mode="r"):
        self.sync(src)
        with self.local.open(src, mode) as f:
            yield f

    def save(self, file, dest, emit=True):
        self.local.save(file, dest, emit=True)
        self.sync(dest)

    def sync(self, src):
        local = self.local.exists(src)
        upstream = self.upstream.exists(src)

        if local != upstream:
            if local:
                with self.local.open(src=src) as f:
                    self.upstream.save(f, dest=src, emit=False)
            else:
                with self.upstream.open(src=src) as f:
                    self.local.save(f, dest=src, emit=False)

    def delete(self, src, on_err: lambda src, err: None):
        """
        片側のファイルが存在しないとき、削除すべきか残すべきか情報が残らない。
        """
        tmp = self.local.move_to_tmp(src)
        try:
            self.upstream.delete(src)
        except Exception as e:
            try:
                on_err(src, e)
            except Exception:
                ...
            self.save(tmp, emit=False)  # 失敗したら元に戻す
            raise e
        tmp.delete()


# 簡易的な全文検索エンジン
# https://github.com/mchaput/whoosh

# rust製の全文検索エンジン（pythonクライアントあり）
# https://github.com/quickwit-oss/tantivy
