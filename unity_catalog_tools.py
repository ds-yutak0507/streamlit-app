from typing import List, Dict
from databricks.sdk import WorkspaceClient


class UnityCatalogClient:
    """Unity Catalogのテーブル情報を取得するクライアント"""

    def __init__(self, workspace_client: WorkspaceClient):
        self.w = workspace_client

    def list_tables(self, catalog: str, schema: str) -> List[Dict]:
        """スキーマ内のテーブル一覧を取得

        Args:
            catalog: カタログ名
            schema: スキーマ名

        Returns:
            テーブル情報のリスト [{"name": "table1", "table_type": "MANAGED"}, ...]
        """
        try:
            tables = self.w.tables.list(
                catalog_name=catalog,
                schema_name=schema
            )

            result = []
            for table in tables:
                result.append({
                    "name": table.name,
                    "table_type": table.table_type.value if table.table_type else "UNKNOWN",
                    "comment": table.comment or ""
                })

            return result
        except Exception as e:
            raise RuntimeError(f"テーブル一覧の取得に失敗しました: {str(e)}")

    def get_table_details(self, catalog: str, schema: str, table: str) -> Dict:
        """テーブルの詳細情報を取得（カラム名、型、コメント）

        Args:
            catalog: カタログ名
            schema: スキーマ名
            table: テーブル名

        Returns:
            テーブル詳細情報の辞書
        """
        try:
            full_name = f"{catalog}.{schema}.{table}"
            table_info = self.w.tables.get(full_name=full_name)

            columns = []
            if table_info.columns:
                for col in table_info.columns:
                    columns.append({
                        "name": col.name,
                        "type": col.type_name.value if col.type_name else "UNKNOWN",
                        "comment": col.comment or ""
                    })

            result = {
                "full_name": full_name,
                "name": table_info.name,
                "catalog": table_info.catalog_name,
                "schema": table_info.schema_name,
                "table_type": table_info.table_type.value if table_info.table_type else "UNKNOWN",
                "comment": table_info.comment or "",
                "columns": columns
            }

            return result
        except Exception as e:
            raise RuntimeError(f"テーブル '{catalog}.{schema}.{table}' の詳細取得に失敗しました: {str(e)}")

    def execute_tool(self, tool_name: str, arguments: Dict) -> str:
        """ツール呼び出しを実行して結果を返す

        Args:
            tool_name: 実行するツール名 ("list_tables" または "get_table_details")
            arguments: ツールの引数

        Returns:
            実行結果を整形した文字列
        """
        try:
            if tool_name == "list_tables":
                catalog = arguments.get("catalog")
                schema = arguments.get("schema")

                if not catalog or not schema:
                    return "Error: catalog と schema は必須パラメータです"

                tables = self.list_tables(catalog, schema)

                if not tables:
                    return f"スキーマ {catalog}.{schema} にはテーブルが見つかりませんでした。"

                # 読みやすい形式に整形
                result = f"スキーマ {catalog}.{schema} 内のテーブル一覧:\n\n"
                for table in tables:
                    result += f"- {table['name']} ({table['table_type']})"
                    if table['comment']:
                        result += f": {table['comment']}"
                    result += "\n"

                return result

            elif tool_name == "get_table_details":
                catalog = arguments.get("catalog")
                schema = arguments.get("schema")
                table = arguments.get("table")

                if not catalog or not schema or not table:
                    return "Error: catalog, schema, table は必須パラメータです"

                details = self.get_table_details(catalog, schema, table)

                # 読みやすい形式に整形
                result = f"テーブル: {details['full_name']}\n"
                result += f"タイプ: {details['table_type']}\n"
                if details['comment']:
                    result += f"説明: {details['comment']}\n"
                result += f"\nカラム情報:\n"

                for col in details['columns']:
                    result += f"- {col['name']} ({col['type']})"
                    if col['comment']:
                        result += f": {col['comment']}"
                    result += "\n"

                return result
            else:
                return f"Error: 不明なツール名 '{tool_name}'"

        except RuntimeError as e:
            # 既に整形されたエラーメッセージ
            return f"エラー: {str(e)}"
        except Exception as e:
            # 予期しないエラー
            return f"予期しないエラーが発生しました: {str(e)}"


def get_function_definitions() -> List[Dict]:
    """OpenAI互換のFunction定義を返す

    Returns:
        LLMに渡すツール定義のリスト
    """
    return [
        {
            "type": "function",
            "function": {
                "name": "list_tables",
                "description": "Unity Catalogスキーマ内のテーブル一覧を取得します。スキーマに含まれる全てのテーブル名とタイプが返されます。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "catalog": {
                            "type": "string",
                            "description": "カタログ名（例: yuta_kikkawa）"
                        },
                        "schema": {
                            "type": "string",
                            "description": "スキーマ名（例: demo_sales）"
                        }
                    },
                    "required": ["catalog", "schema"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_table_details",
                "description": "Unity Catalogの特定テーブルの詳細情報を取得します。カラム名、データ型、コメントなどが返されます。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "catalog": {
                            "type": "string",
                            "description": "カタログ名（例: yuta_kikkawa）"
                        },
                        "schema": {
                            "type": "string",
                            "description": "スキーマ名（例: demo_sales）"
                        },
                        "table": {
                            "type": "string",
                            "description": "テーブル名"
                        }
                    },
                    "required": ["catalog", "schema", "table"]
                }
            }
        }
    ]
