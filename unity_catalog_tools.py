from typing import List, Dict
import os
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

    def _execute_sql(self, query: str, catalog: str, schema: str) -> Dict:
        """SQL クエリを実行して結果を返す（内部メソッド）

        Args:
            query: 実行する SQL クエリ
            catalog: カタログ名
            schema: スキーマ名

        Returns:
            {
                "columns": ["col1", "col2", ...],
                "rows": [
                    ["value1", "value2", ...],
                    ...
                ]
            }
        """
        try:
            warehouse_id = os.environ.get("WAREHOUSE_ID")
            if not warehouse_id:
                raise RuntimeError("環境変数 WAREHOUSE_ID が設定されていません")

            # SQL を実行
            response = self.w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                catalog=catalog,
                schema=schema,
                wait_timeout="30s"
            )

            # カラム名を取得
            columns = []
            if response.manifest and response.manifest.schema and response.manifest.schema.columns:
                columns = [col.name for col in response.manifest.schema.columns]

            # データを取得
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array

            return {
                "columns": columns,
                "rows": rows
            }

        except Exception as e:
            raise RuntimeError(f"SQL の実行に失敗しました: {str(e)}")

    def get_related_tables(self, catalog: str, schema: str, table: str) -> Dict:
        """テーブルに関連する他のテーブルを取得

        カラム名のパターン（_idで終わるカラム）から関連テーブルを推測します。

        Args:
            catalog: カタログ名
            schema: スキーマ名
            table: テーブル名

        Returns:
            {
                "table": "customers",
                "referenced_by": [
                    {
                        "table": "orders",
                        "constraint": "orders_customer_fk",
                        "columns": [
                            {"source": "customer_id", "target": "customer_id"}
                        ]
                    }
                ],
                "references": [
                    {
                        "table": "countries",
                        "constraint": "customers_country_fk",
                        "columns": [
                            {"source": "country_id", "target": "country_id"}
                        ]
                    }
                ]
            }
        """
        try:
            # このテーブルが参照しているテーブル（このテーブルの_idカラムが他のテーブルにも存在する）
            # カラム名からテーブル名を推測（例: customer_id → dim_customer）
            # 外部キーが明示的に定義されている場合、そのようにSQLを修正した方がいい
            references_query = f"""
            SELECT DISTINCT
                c1.column_name as source_column,
                c2.table_name as target_table,
                c2.column_name as target_column
            FROM information_schema.columns c1
            JOIN information_schema.columns c2
                ON c1.column_name = c2.column_name
                AND c1.table_name != c2.table_name
                AND c1.table_schema = c2.table_schema
                AND c1.table_catalog = c2.table_catalog
            WHERE c1.table_catalog = '{catalog}'
                AND c1.table_schema = '{schema}'
                AND c1.table_name = '{table}'
                AND c1.column_name LIKE '%_id'
                AND LOWER(c2.table_name) LIKE CONCAT('%', REPLACE(LOWER(c1.column_name), '_id', ''), '%')
            ORDER BY target_table, source_column
            """

            references_result = self._execute_sql(references_query, catalog, schema)

            # このテーブルを参照しているテーブル（他のテーブルの_idカラムがこのテーブルにも存在する）
            # カラム名がこのテーブル名に対応する場合のみ（例: fct_order ← order_id）
            # 外部キーが明示的に定義されている場合、そのようにSQLを修正した方がいい
            referenced_by_query = f"""
            SELECT DISTINCT
                c1.table_name as source_table,
                c1.column_name as source_column,
                c2.column_name as target_column
            FROM information_schema.columns c1
            JOIN information_schema.columns c2
                ON c1.column_name = c2.column_name
                AND c1.table_name != c2.table_name
                AND c1.table_schema = c2.table_schema
                AND c1.table_catalog = c2.table_catalog
            WHERE c2.table_catalog = '{catalog}'
                AND c2.table_schema = '{schema}'
                AND c2.table_name = '{table}'
                AND c1.column_name LIKE '%_id'
                AND c1.table_name != '{table}'
                AND LOWER('{table}') LIKE CONCAT('%', REPLACE(LOWER(c1.column_name), '_id', ''), '%')
            ORDER BY source_table, source_column
            """

            referenced_by_result = self._execute_sql(referenced_by_query, catalog, schema)

            # references をグループ化（テーブルごと）
            references = {}
            for row in references_result["rows"]:
                source_column, target_table, target_column = row
                if target_table not in references:
                    references[target_table] = {
                        "table": target_table,
                        "constraint": f"{table}_{target_table}_inferred",
                        "columns": []
                    }
                references[target_table]["columns"].append({
                    "source": source_column,
                    "target": target_column
                })

            # referenced_by をグループ化（テーブルごと）
            referenced_by = {}
            for row in referenced_by_result["rows"]:
                source_table, source_column, target_column = row
                if source_table not in referenced_by:
                    referenced_by[source_table] = {
                        "table": source_table,
                        "constraint": f"{source_table}_{table}_inferred",
                        "columns": []
                    }
                referenced_by[source_table]["columns"].append({
                    "source": source_column,
                    "target": target_column
                })

            return {
                "table": table,
                "referenced_by": list(referenced_by.values()),
                "references": list(references.values())
            }

        except Exception as e:
            raise RuntimeError(f"テーブル '{catalog}.{schema}.{table}' の関連テーブル取得に失敗しました: {str(e)}")

    def execute_tool(self, tool_name: str, arguments: Dict) -> str:
        """ツール呼び出しを実行して結果を返す

        Args:
            tool_name: 実行するツール名 ("list_tables", "get_table_details", "get_related_tables")
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

            elif tool_name == "get_related_tables":
                catalog = arguments.get("catalog")
                schema = arguments.get("schema")
                table = arguments.get("table")

                if not catalog or not schema or not table:
                    return "Error: catalog, schema, table は必須パラメータです"

                # 関連テーブルを取得
                related = self.get_related_tables(catalog, schema, table)

                # 読みやすい形式に整形
                result = f"テーブル {catalog}.{schema}.{table} の関連情報:\n\n"

                # このテーブルを参照しているテーブル
                if related["referenced_by"]:
                    result += "【このテーブルを参照しているテーブル】\n"
                    for ref in related["referenced_by"]:
                        result += f"- {ref['table']}:\n"
                        for col in ref["columns"]:
                            result += f"  {ref['table']}.{col['source']} → {table}.{col['target']}\n"
                    result += "\n"
                else:
                    result += "【このテーブルを参照しているテーブル】\n（なし）\n\n"

                # このテーブルが参照しているテーブル
                if related["references"]:
                    result += "【このテーブルが参照しているテーブル】\n"
                    for ref in related["references"]:
                        result += f"- {ref['table']}:\n"
                        for col in ref["columns"]:
                            result += f"  {table}.{col['source']} → {ref['table']}.{col['target']}\n"
                    result += "\n"
                else:
                    result += "【このテーブルが参照しているテーブル】\n（なし）\n\n"

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
        },
        {
            "type": "function",
            "function": {
                "name": "get_related_tables",
                "description": """Unity Catalog の特定テーブルに関連する他のテーブルを取得します。
外部キー制約に基づいて、このテーブルを参照しているテーブル、およびこのテーブルが参照しているテーブルを返します。

例:
- 「customers テーブルに関連するテーブルは？」
- 「orders テーブルを参照しているテーブルは？」
- 「products テーブルが参照しているテーブルは？」""",
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
