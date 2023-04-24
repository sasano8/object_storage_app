from store import AbstractStorage

GLOBAL_INSTANCE: AbstractStorage = None


# 実行パスのコンフィグを見て自動でバックエンドストレージを設定する


def ll(src: str = ""):
    return GLOBAL_INSTANCE.ll(src)
