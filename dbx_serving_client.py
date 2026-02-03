from typing import List, Dict, Optional
from databricks.sdk import WorkspaceClient
from openai import OpenAI
import json

class DatabricksServingChatClient:
    """
    Databricks OpenAI クライアントで Serving Endpoint を呼ぶクライアント。
    """
    def __init__(self, workspace_client: WorkspaceClient, endpoint_name: str, unity_catalog_client: Optional['UnityCatalogClient'] = None):
        self.w = workspace_client
        self.endpoint_name = endpoint_name
        self.uc_client = unity_catalog_client

        # Databricks SDKでトークン取得
        headers = self.w.config.authenticate()
        if not headers or "Authorization" not in headers:
            raise RuntimeError("Authorization header not available.")

        # "Bearer xxx" → "xxx" にする
        api_key = headers["Authorization"].replace("Bearer ", "").strip()

        base_url = f"{self.w.config.host.rstrip('/')}/serving-endpoints"

        self.client = OpenAI(api_key=api_key, base_url=base_url)

    def send_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ) -> str:
        resp = self.client.chat.completions.create(
            model=self.endpoint_name,
            messages=messages,
            temperature=float(temperature),
            max_tokens=int(max_tokens),
        )
        return self._extract_text(resp)

    def _extract_text(self, resp) -> str:
        """OpenAI互換レスポンスからテキストを取り出す"""
        try:
            return resp.choices[0].message.content
        except Exception as e:
            raise ValueError(f"Unexpected response format: {resp}") from e

    def send_chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        max_iterations: int = 5
    ) -> str:
        """Function Calling対応のチャット送信

        フロー:
        1. ツール定義付きでLLMを呼び出し
        2. LLMがツールを呼び出すか確認
        3. ツールを実行して結果をメッセージに追加
        4. LLMに再度送信（最終回答が返るまで繰り返し）

        Args:
            messages: メッセージ履歴
            temperature: 温度パラメータ
            max_tokens: 最大トークン数
            max_iterations: 最大反復回数（デフォルト5回）

        Returns:
            LLMの最終的な回答テキスト
        """
        # UC clientが設定されていない場合は通常のチャットにフォールバック
        if not self.uc_client:
            return self.send_chat(messages, temperature, max_tokens)

        from unity_catalog_tools import get_function_definitions

        tool_definitions = get_function_definitions()
        working_messages = messages.copy()

        for iteration in range(max_iterations):
            # ツール定義付きでLLM呼び出し
            response = self.client.chat.completions.create(
                model=self.endpoint_name,
                messages=working_messages,
                temperature=float(temperature),
                max_tokens=int(max_tokens),
                tools=tool_definitions,
                tool_choice="auto"
            )

            message = response.choices[0].message

            # ツール呼び出しがある場合
            if message.tool_calls:
                # Assistantのメッセージを追加
                working_messages.append({
                    "role": "assistant",
                    "content": message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": tc.type,
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments
                            }
                        }
                        for tc in message.tool_calls
                    ]
                })

                # 各ツール呼び出しを実行
                for tool_call in message.tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)

                    # ツール実行
                    try:
                        tool_result = self.uc_client.execute_tool(
                            function_name,
                            function_args
                        )
                    except Exception as e:
                        tool_result = f"Error executing tool: {str(e)}"

                    # ツール結果をメッセージに追加
                    working_messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": tool_result
                    })
            else:
                # 最終的なテキスト回答が返された
                return message.content or ""

        # 最大反復回数に達した
        return "申し訳ありませんが、処理が制限回数に達しました。"
