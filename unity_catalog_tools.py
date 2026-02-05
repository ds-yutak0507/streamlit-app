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

            # 結果を待機
            result = response.result

            # カラム名を取得
            columns = []
            if result.manifest and result.manifest.schema and result.manifest.schema.columns:
                columns = [col.name for col in result.manifest.schema.columns]

            # データを取得
            rows = []
            if result.result and result.result.data_array:
                rows = result.result.data_array

            return {
                "columns": columns,
                "rows": rows
            }

        except Exception as e:
            raise RuntimeError(f"SQL の実行に失敗しました: {str(e)}")

    def get_related_tables(self, catalog: str, schema: str, table: str) -> Dict:
        """テーブルに関連する他のテーブルを取得

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
            # このテーブルを参照している外部キーを取得
            referenced_by_query = f"""
            SELECT
                tc.table_name as source_table,
                tc.constraint_name,
                fk_col.column_name as source_column,
                pk_col.column_name as target_column
            FROM information_schema.referential_constraints rc
            JOIN information_schema.table_constraints tc
                ON rc.constraint_name = tc.constraint_name
                AND rc.constraint_schema = tc.constraint_schema
            JOIN information_schema.table_constraints pk_tc
                ON rc.unique_constraint_name = pk_tc.constraint_name
                AND rc.unique_constraint_schema = pk_tc.constraint_schema
            JOIN information_schema.key_column_usage fk_col
                ON tc.constraint_name = fk_col.constraint_name
                AND tc.constraint_schema = fk_col.constraint_schema
            JOIN information_schema.key_column_usage pk_col
                ON pk_tc.constraint_name = pk_col.constraint_name
                AND pk_tc.constraint_schema = pk_col.constraint_schema
                AND fk_col.ordinal_position = pk_col.ordinal_position
            WHERE pk_tc.table_name = '{table}'
                AND pk_tc.table_schema = '{schema}'
                AND pk_tc.table_catalog = '{catalog}'
            ORDER BY tc.table_name, tc.constraint_name, fk_col.ordinal_position
            """

            referenced_by_result = self._execute_sql(referenced_by_query, catalog, schema)

            # このテーブルが参照している外部キーを取得
            references_query = f"""
            SELECT
                tc.constraint_name,
                fk_col.column_name as source_column,
                pk_tc.table_name as target_table,
                pk_col.column_name as target_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.constraint_schema = rc.constraint_schema
            JOIN information_schema.table_constraints pk_tc
                ON rc.unique_constraint_name = pk_tc.constraint_name
                AND rc.unique_constraint_schema = pk_tc.constraint_schema
            JOIN information_schema.key_column_usage fk_col
                ON tc.constraint_name = fk_col.constraint_name
                AND tc.constraint_schema = fk_col.constraint_schema
            JOIN information_schema.key_column_usage pk_col
                ON pk_tc.constraint_name = pk_col.constraint_name
                AND pk_tc.constraint_schema = pk_col.constraint_schema
                AND fk_col.ordinal_position = pk_col.ordinal_position
            WHERE tc.table_name = '{table}'
                AND tc.table_schema = '{schema}'
                AND tc.table_catalog = '{catalog}'
                AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.constraint_name, fk_col.ordinal_position
            """

            references_result = self._execute_sql(references_query, catalog, schema)

            # referenced_by をグループ化
            referenced_by = {}
            for row in referenced_by_result["rows"]:
                source_table, constraint_name, source_column, target_column = row
                if constraint_name not in referenced_by:
                    referenced_by[constraint_name] = {
                        "table": source_table,
                        "constraint": constraint_name,
                        "columns": []
                    }
                referenced_by[constraint_name]["columns"].append({
                    "source": source_column,
                    "target": target_column
                })

            # references をグループ化
            references = {}
            for row in references_result["rows"]:
                constraint_name, source_column, target_table, target_column = row
                if constraint_name not in references:
                    references[constraint_name] = {
                        "table": target_table,
                        "constraint": constraint_name,
                        "columns": []
                    }
                references[constraint_name]["columns"].append({
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
                        result += f"- {ref['table']} (制約: {ref['constraint']})\n"
                        for col in ref["columns"]:
                            result += f"  {ref['table']}.{col['source']} → {table}.{col['target']}\n"
                    result += "\n"
                else:
                    result += "【このテーブルを参照しているテーブル】\n（なし）\n\n"

                # このテーブルが参照しているテーブル
                if related["references"]:
                    result += "【このテーブルが参照しているテーブル】\n"
                    for ref in related["references"]:
                        result += f"- {ref['table']} (制約: {ref['constraint']})\n"
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
