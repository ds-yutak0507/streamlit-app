## アプリについて
Databricks Apps学習用に制作したチャットアプリ
## 使用ツール
- Streamlit (Front)
- Databricks (Back)
    - Serving: LLMのエンドポイントを設定
    - Compute -> Apps: Webアプリの立ち上げ | エンドポイントの紐づけ
## ブランチについて
### version1
- チャットアプリとしてDatabricksのLLMを使用しAIと会話ができる
### version2
変更点
1. Class化しファイル分け
2. エンドポイントの呼び出しにSDK(Databricks OpenAI client)を使用
### version3
変更点
1. 以下のUnity Catalog情報取得に特化
    - Catalog;: yuta_kikkawa
    - Schema: demo_sales
2. 機能を以下に絞る
    - テーブルの一覧を取得
    - テーブルの詳細を取得
    - テーブルの関連性を取得
