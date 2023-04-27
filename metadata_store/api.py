from http import client
from threading import local
from unittest import result

# from ..strategy import Pipeline, Storage

from fastapi import APIRouter, Header, Response
from fastapi.responses import StreamingResponse, FileResponse

# from ..utils import Md5Digest

from fastapi import File, UploadFile, Form

from typing_extensions import Annotated
from typing import final, List, Dict, Any
import os
import asyncer

import fsspec

from .store import LocalStorage, EncryptedStorage, CompressionStorage, AsyncLocalStorage
from typing import Literal


# client = LocalStorage(os.path.dirname(__file__) + "/../tests/dir_asgi")
client = AsyncLocalStorage(os.path.dirname(__file__) + "/../tests/dir_asgi")
# client = EncryptedStorage(client)
# client = CompressionStorage(client)

# print(fsspec.available_protocols())


router = APIRouter()


@router.get("/inspect")
async def inspect(src: str = ""):
    return client.get_storage_pipe()


@router.get("/ll")
async def ll(src: str = ""):
    result = []
    async for x in client.ll(src):
        result.append(x)
    return result


@router.get("/info")
async def info(src: str = "") -> dict:
    return await client.info(src)


@router.get("/exists")
async def exists(src: str = ""):
    return await client.exists(src)


@router.get("/join")
async def join(src: str = ""):
    return client._join(src)


@router.post("/create")
async def create(
    file: Annotated[UploadFile, File(description="A file read as UploadFile")],
    dest: str = "",
    meta: Annotated[dict, Form()] = {},
    etag: Annotated[str, Header()] = "",
    content_encoding: Annotated[Literal["gzip", "deflate", "br"], Header()] = "gzip",
    content_md5: Annotated[str, Header()] = "",
    # extract_archive: Annotated[bool, Header(description="サーバー側でアーカイブを展開して保持するか指定します")] = False,
    archive_format: Annotated[
        Literal["tar.gz"], Header(description="サーバー側でアーカイブを展開させる場合、想定するアーカイブ形式を指定します")
    ] = None,
    keep_ext: Annotated[
        Literal[False],
        Header(description="サーバー側でアーカイブを展開する場合、archive_formatで指定した形式を除去するか指定します"),
    ] = False,
):
    """
    zip: 解凍後のサイズに関する情報をアーカイブ内のファイルヘッダーに格納しています
    tar: 解凍後のサイズに関する情報をアーカイブ内のファイルヘッダーに格納しています
    tar.gz: 圧縮されているため、サイズを知ることができません
    """
    return await client.create(file, dest=dest)


# compression
# gzip
# deflate
# br  (Brotli)

# grpc-encoding


@router.post("/put")
async def put(
    file: Annotated[UploadFile, File(description="A file read as UploadFile")],
    dest: str = "",
    meta: Annotated[dict, Form()] = {},
    etag: Annotated[str, Header()] = "",
    content_encoding: Annotated[Literal["gzip", "deflate", "br"], Header()] = "gzip",
    content_md5: Annotated[str, Header()] = "",
    # extract_archive: Annotated[bool, Header(description="サーバー側でアーカイブを展開して保持するか指定します")] = False,
    archive_format: Annotated[
        Literal["tar.gz"], Header(description="サーバー側でアーカイブを展開させる場合、想定するアーカイブ形式を指定します")
    ] = None,
    keep_ext: Annotated[
        Literal[False],
        Header(description="サーバー側でアーカイブを展開する場合、archive_formatで指定した形式を除去するか指定します"),
    ] = False,
):
    """
    zip: 解凍後のサイズに関する情報をアーカイブ内のファイルヘッダーに格納しています
    tar: 解凍後のサイズに関する情報をアーカイブ内のファイルヘッダーに格納しています
    tar.gz: 圧縮されているため、サイズを知ることができません
    """
    return await client.put(file, dest=dest)


from fastapi.responses import StreamingResponse


@router.post("/load")
async def open(src: str = "", mode: Literal["rb"] = "rb"):
    async def generate():
        async with await client.open(src, mode) as f:
            async for x in f:
                yield x

    return StreamingResponse(generate(), media_type="application/octet-stream")


class StramRes(StreamingResponse):
    media_type = "application/octet-stream"


@router.post(
    "/load",
    response_class=StramRes,
    responses={
        200: {
            "description": "returns a binary file.",
        }
    },
)
async def load(src: str = "", mode: Literal["r", "rb"] = "rb"):
    if mode == "r":
        return client.open(src, mode)
    elif mode == "rb":
        return client.open(src, mode)
    else:
        raise Exception()


# ファイルをダウンロードさせるとき、Content-Disposition に attachment; をつける方式と、application/octet-stream をつける方式がある


@router.post("/delete")
async def delete(src: str = ""):
    return await client.delete(src)


@router.options("/create")
async def upload_options(response: Response):
    response.headers["Access-Control-Allow-Methods"] = "OPTIONS, POST"
    response.headers[
        "Access-Control-Allow-Headers"
    ] = "Content-Type, Content-Encoding, Max-Upload-Size, Archive-Handling-Mode, Accepted-Archive-Formats"
    response.headers["Max-Upload-Size"] = str(1024 * 1024 * 10)  # 10 MB
    response.headers[
        "Archive-Handling-Mode"
    ] = "no-extract, extract"  # 展開してサーバーに置くか否か（ファイルとディレクトリの場合で変えてもらう）
    response.headers["Accepted-Archive-Formats"] = "tar.gz, zip"  # サポートするアーカイブ
    response.headers["Archive-Name-Mode"] = "keep"  # サーバー側で拡張子を除去するか否か  # remove_ext
    return response

    # - Access-Control-Allow-Methods: サポートされているHTTPメソッドを示します。例：GET, POST, OPTIONS CORSに関連するヘッダーの一部
    # - Access-Control-Allow-Headers: クライアントから受け付けるヘッダーを示します。例：Content-Type, Content-Encoding, Accept-Encoding CORSに関連するヘッダーの一部
    # - (非標準ヘッダー)Max-Upload-Size: サーバーが受け付ける最大ファイルサイズを示します。例：10485760（10MB）
    # - (非標準ヘッダー)Accepted-Archive-Formats: サーバーが受け付けるアーカイブ形式を示します。例：tar.gz, zip
    # - (非標準ヘッダー)Archive-Handling-Mode: サーバーがアーカイブをどのように取り扱うかを示します。例：archive-as-is（アーカイブのまま保存）、extract-and-store（展開して保存）


"""
# アップロード時：

## 1-1. クライアントがファイル送信に関するサーバーのスペックを知る方法

クライアントはOPTIONSリクエストを使用してサーバーのサポートする圧縮形式を確認できます。サーバーはAccept-Encodingヘッダーをレスポンスに含めて返すべきです。

## 1-2. クライアント側がアップロードする前にする前処理

クライアントは、サーバーがサポートする圧縮形式に基づいて、適切な圧縮アルゴリズムを選択し、ファイルを圧縮します。

## 1-3. クライアントがアップロードのHTTPリクエストを送信する時の状態

クライアントは、圧縮されたファイルを送信し、リクエストヘッダーにContent-Encodingヘッダーを含めます。これにより、使用された圧縮アルゴリズムがサーバーに通知されます。

## 1-4. サーバーがアップロードのリクエストを受信し、どのようにファイルを処理するか

サーバーは、リクエストヘッダーのContent-Encoding情報を使用して、適切な圧縮解除アルゴリズムを選択し、受信したファイルを解凍します。その後、サーバーはファイルをそのままストレージに保存します。

## 1-5. どのようなレスポンスを返すか

アップロードが成功した場合、サーバーは通常、200 OKまたは201 Createdのステータスコードを含むレスポンスを返します。

# ダウンロード時：

## 2-1. クライアントがファイル受信に関するサーバーのスペックを知る方法

クライアントはOPTIONSリクエストを使用してサーバーのサポートする圧縮形式を確認できます。サーバーはAccept-Encodingヘッダーをレスポンスに含めて返すべきです。

## 2-2. クライアントがダウンロードのHTTPリクエストを送信する時の状態

クライアントは、HTTPリクエストでAccept-Encodingヘッダーを設定し、サポートする圧縮形式をサーバーに通知します。

## 2-3. サーバーがリクエストに基づき、レスポンスする前に行う処理

サーバーは、クライアントがサポートする圧縮形式に基づいて、適切な圧縮アルゴリズムを選択し、ファイルを圧縮します。

## 2-4. どのようなレスポンスを返すか

サーバーは、圧縮されたファイルを送信し、レスポンスヘッダーにContent-Encodingヘッダーを含めます。これにより、使用された圧縮アルゴリズムがクライアントに通知されます。通常、ステータスコードは200 OKが返されます。
ファイルとフォルダの管理について：

サーバー側でフォルダをアーカイブして保存するかどうかは、実装によって異なります。以下に、いくつかの一般的な方法を示します。

サーバー側でアーカイブされたフォルダを保存
クライアントは、アップロード時にフォルダをアーカイブ（例：tar.gz）し、アップロードします。
サーバーは、アーカイブされたフォルダをそのまま保存します。
ダウンロード時、サーバーはアーカイブされたフォルダをクライアントに送信し、クライアントはそれを解凍します。
クライアント側でアーカイブし、サーバー側で展開
クライアントは、アップロード時にフォルダをアーカイブ（例：tar.gz）し、アップロードします。
サーバーは、アーカイブされたフォルダを受信し、展開して保存します。
ダウンロード時、サーバーは必要に応じてフォルダを再度アーカイブし、クライアントに送信します。クライアントはそれを解凍します。
どちらの方法を選択するかは、システムの要件やパフォーマンス上の考慮に基づいて決定します。例えば、サーバー側でアーカイブされたフォルダを保存する方法は、ストレージスペースの節約ができる一方で、フォルダ内の個別のファイルへのアクセスにはアーカイブの展開が必要となります。それに対して、クライアント側でアーカイブし、サーバー側で展開する方法は、サーバー側で個別のファイルへのアクセスが容易になりますが、ストレージスペースの使用量が増加する可能性があります。

最終的に、オブジェクトストレージシステムの設計と実装に関しては、以下の要素を考慮することが重要です：

ストレージ容量とパフォーマンスのバランス
圧縮・解凍の負荷分散（クライアント側とサーバー側）
ファイル・フォルダの管理方法（アーカイブや展開の方法）
柔軟性と拡張性（新しい圧縮アルゴリズムやストレージ管理方法の追加）
これらの要素を総合的に検討し、システムの要件に最適な設計と実装を行うことが重要です。また、適切なエラー処理やリカバリ機能も実装することで、システムの信頼性と安定性を向上させることができます。



アーカイブの取り扱いに関するユースケースは、主に以下のようになります。

クライアント側で圧縮、サーバー側でそのまま保存、クライアント側にそのまま返す
クライアント側で圧縮、サーバー側で展開して保存、クライアント側に圧縮して返す
クライアント側で圧縮せず、サーバー側で圧縮して保存、クライアント側に圧縮して返す
クライアント側で圧縮せず、サーバー側でそのまま保存、クライアント側に圧縮せず返す


アーカイブの取り扱いに関するユースケースは、主に以下のようになります。

クライアント側で圧縮、サーバー側でそのまま保存、クライアント側にそのまま返す
クライアント側で圧縮、サーバー側で展開して保存、クライアント側に圧縮して返す
クライアント側で圧縮せず、サーバー側で圧縮して保存、クライアント側に圧縮して返す
クライアント側で圧縮せず、サーバー側でそのまま保存、クライアント側に圧縮せず返す
あなたがやりたいことは、HTTPクライアントのラッパーを使用して、ディレクトリが引数として渡された場合に自動的にアーカイブして送信し、ダウンロード時にディレクトリとして展開する機能を実現することです。これは実現可能であり、サーバーとクライアントがどのようにアーカイブを取り扱うかについて明確な規則を定めることで、管理が難しくなることはありません。

たとえば、以下のような方法で実現できます。

クライアント側でディレクトリを圧縮（アーカイブ）し、サーバーに送信します。
サーバー側は、アーカイブされたディレクトリをそのまま保存します。
ダウンロード時、サーバーはアーカイブされたディレクトリをクライアントに送信します。
クライアントは、受信したアーカイブされたディレクトリを展開します。
この方法では、サーバーとクライアントの両方がアーカイブされたディレクトリをそのまま取り扱います。これにより、ストレージ容量を節約できる一方で、サーバー側で個別のファイルへのアクセスが必要な場合はアーカイブの展開が必要となります。

サーバーとクライアント間で明確な規則を定め、それに従って実装を行うことで、アーカイブの取り扱いに関する管理が難しくなることはありません。ただし、エラー処理やリカバリ機能も適切に実装しておくことが
重要です。これにより、アーカイブの処理中に問題が発生した場合でも、システムの信頼性と安定性を維持できます。

また、クライアントとサーバー間の通信で、アーカイブされたディレクトリがどのような形式で送られているかを示すメタデータを付与することで、受信側が適切な処理を行えるようにします。これには、HTTPヘッダーやリクエスト・レスポンスのパラメータを使用できます。

実装の際には、以下の点に注意してください。

圧縮・解凍のアルゴリズムとバージョンの互換性：クライアントとサーバーが同じアルゴリズムとバージョンで圧縮・解凍を行えるようにします。
エラー処理：アーカイブの圧縮・解凍中にエラーが発生した場合の処理を適切に実装し、システムの信頼性と安定性を保ちます。
パフォーマンス：アーカイブの圧縮・解凍にかかる時間やリソースを最小限に抑えるため、効率的なアルゴリズムを選択します。
セキュリティ：クライアントとサーバー間の通信を暗号化し、アーカイブデータの漏洩や改ざんを防止します。
これらの点を考慮しながら、サーバーとクライアントの両方でアーカイブを適切に取り扱い、システムの要件に合った実装を行うことが重要です。


- OPTIONS /v1/upload
    - Access-Control-Allow-Methods: サポートされているHTTPメソッドを示します。例：GET, POST, OPTIONS CORSに関連するヘッダーの一部
    - Access-Control-Allow-Headers: クライアントから受け付けるヘッダーを示します。例：Content-Type, Content-Encoding, Accept-Encoding CORSに関連するヘッダーの一部
    - (非標準ヘッダー)Max-Upload-Size: サーバーが受け付ける最大ファイルサイズを示します。例：10485760（10MB）
    - (非標準ヘッダー)Accepted-Archive-Formats: サーバーが受け付けるアーカイブ形式を示します。例：tar.gz, zip
    - (非標準ヘッダー)Archive-Handling-Mode: サーバーがアーカイブをどのように取り扱うかを示します。例：archive-as-is（アーカイブのまま保存）、extract-and-store（展開して保存）
- OPTIONS /v1/download
    - Access-Control-Allow-Methods: サポートされているHTTPメソッドを示します。例：GET, POST, OPTIONS CORSに関連するヘッダーの一部
    - Access-Control-Allow-Headers: クライアントから受け付けるヘッダーを示します。例：Content-Type, Content-Encoding, Accept-Encoding CORSに関連するヘッダーの一部
    - (非標準ヘッダー)Max-Upload-Size: サーバーが受け付ける最大ファイルサイズを示します。例：10485760（10MB）
    - (非標準ヘッダー)Accepted-Archive-Formats: サーバーが受け付けるアーカイブ形式を示します。例：tar.gz, zip
    - (非標準ヘッダー)Archive-Handling-Mode: サーバーがアーカイブをどのように取り扱うかを示します。例：archive-as-is（アーカイブのまま保存）、extract-and-store（展開して保存）

非標準ヘッダーは X- プレフィックスをつけるのが過去ではプラクティスだったが、近年ではつけないことを推奨している。
X- ヘッダーが標準になった時、移行が楽。

"""


"""
メタデータを管理しない方式
ローカルファイルシステム

メタデータを管理する方式
メターデータ管理サーバ -> ローカルファイルシステム
"""


# client.distribute(
#     "asdfas",
#     storage["a"],
#     storage["b"],
#     storage["c"],
#     storage["d"],
#     storage["e"],
# )


# class Downloader:
#     def ll(self):
#         ...

#     def download(self):
#         ...

# class Distributor:
#     def __init__(self, *downloadables):
#         self.downloadables = downloadables

#     def download(self):
#         items = self.downloadables.ll()
#         for item in items:
#             item.download()


def create_signing_key(secret_key: str, datestamp, region, service):
    import hmac
    import hashlib
    import datetime

    # hmac(Hash Based Message Authentication Code)
    k_date = hmac.new(
        b"AWS4" + secret_key.encode("utf-8"), datestamp.encode("utf-8"), hashlib.sha256
    ).digest()
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing


from fastapi import Request


def verify_sigv4_signature(request: Request, x_amz_date: str, authorization: str):
    import hashlib
    import hmac

    signed_headers = authorization.split("SignedHeaders=")[1].split(",")[0]
    request_method = request.method
    canonical_uri = request.url.path
    canonical_querystring = request.url.query

    canonical_headers = ""
    for header in signed_headers.split(";"):
        canonical_headers += f"{header}:{request.headers[header]}\n"

    payload_hash = hashlib.sha256(request.body).hexdigest()

    canonical_request = f"{request_method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    canonical_request_hash = hashlib.sha256(
        canonical_request.encode("utf-8")
    ).hexdigest()

    datestamp = x_amz_date[:8]
    credential_scope = f"{datestamp}/{AWS_REGION}/s3/aws4_request"

    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{x_amz_date}\n{credential_scope}\n{canonical_request_hash}"
    )

    signing_key = create_signing_key(AWS_SECRET_ACCESS_KEY, datestamp, AWS_REGION, "s3")
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    return signature


async def your_endpoint(
    request: Request, x_amz_date: str = Header(None), authorization: str = Header(None)
):
    if not x_amz_date or not authorization:
        raise HTTPException(status_code=400, detail="Missing required headers")

    try:
        calculated_signature = verify_sigv4_signature(
            request, x_amz_date, authorization
        )
        provided_signature = authorization.split("Signature=")[1]

        if calculated_signature != provided_signature:
            raise HTTPException(status_code=403, detail="Invalid signature")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Valid signature"}


# canonical_request =

# メッセージ認証符号（MAC）とディジタル署名MAC
# HMAC-based One-Time Password
# 署名付きURL: 誰でも取得できる有効期間付きのURLを発行する

# HAMCは送受信者間の完全性の確認と共有鍵に依存した
