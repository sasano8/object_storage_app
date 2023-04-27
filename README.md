# 簡単なオブジェクトストレージを作りたい

# 候補

- svn

  - メタデータを持てる
  - ロックができる
  - ローカルファイルシステムと互換性がある
  - s3ともメタデータを共有できるかも
- hdf5

  - メタデータを持てる
  - ローカルファイルシステムと似ているがファイルを開いたままでないと階層を辿れないので色々問題が起きそう

# 全文検索

- supabase
- HyperDB: OpenAIのAPI KEYが必要でまだ試せてない
- tantivy: 日本語検索トークナイザーが標準でついていない（Linderaと連携できそう）
