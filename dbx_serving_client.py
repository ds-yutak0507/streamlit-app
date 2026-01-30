from typing import List, Dict, Optional
from databricks.sdk import WorkspaceClient
import json

class DatabricksServingChatClient:
    """
    Databricks SDK を直接使用して Serving Endpoint を呼ぶクライアント。
    """
    def __init__(self, workspace_client: WorkspaceClient, endpoint_name: str, unity_catalog_client: Optional['UnityCatalogClient'] = None):
        self.w = workspace_client
        self.endpoint_name = endpoint_name
        self.uc_client = unity_catalog_client

    def send_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ) -> str:
        """Databricks SDKを使用してチャット送信"""
        # Databricks SDK の query メソッドを使用
        query_input = {
            "messages": messages,
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }

        response = self.w.serving_endpoints.query(
            name=self.endpoint_name,
            inputs=[query_input]
        )

        return self._extract_text(response)

    def _extract_text(self, response) -> str:
        """Databricks SDK レスポンスからテキストを取り出す"""
        try:
            # Databricks SDK のレスポンス形式に対応
            if hasattr(response, 'predictions') and response.predictions:
                # predictions形式
                prediction = response.predictions[0]
                if isinstance(prediction, dict):
                    # OpenAI互換レスポンス形式
                    if 'choices' in prediction:
                        return prediction['choices'][0]['message']['content']
                    # 直接テキスト形式
                    elif 'content' in prediction:
                        return prediction['content']
                    elif 'response' in prediction:
                        return prediction['response']
                return str(prediction)

            # その他の形式
            return str(response)
        except Exception as e:
            raise ValueError(f"Unexpected response format: {response}") from e

    def send_chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        max_iterations: int = 5
    ) -> str:
        """Function Calling対応のチャット送信（Databricks SDK使用）

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
            query_input = {
                "messages": working_messages,
                "temperature": float(temperature),
                "max_tokens": int(max_tokens),
                "tools": tool_definitions,
                "tool_choice": "auto"
            }

            response = self.w.serving_endpoints.query(
                name=self.endpoint_name,
                inputs=[query_input]
            )

            # レスポンスからメッセージを抽出
            message = self._extract_message(response)

            # ツール呼び出しがある場合
            if message.get("tool_calls"):
                # Assistantのメッセージを追加
                working_messages.append({
                    "role": "assistant",
                    "content": message.get("content"),
                    "tool_calls": message["tool_calls"]
                })

                # 各ツール呼び出しを実行
                for tool_call in message["tool_calls"]:
                    function_name = tool_call["function"]["name"]
                    function_args = json.loads(tool_call["function"]["arguments"])

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
                        "tool_call_id": tool_call["id"],
                        "content": tool_result
                    })
            else:
                # 最終的なテキスト回答が返された
                return message.get("content", "")

        # 最大反復回数に達した
        return "申し訳ありませんが、処理が制限回数に達しました。"

    def _extract_message(self, response) -> Dict:
        """レスポンスからメッセージオブジェクトを抽出"""
        try:
            if hasattr(response, 'predictions') and response.predictions:
                prediction = response.predictions[0]
                if isinstance(prediction, dict):
                    # OpenAI互換レスポンス形式
                    if 'choices' in prediction:
                        return prediction['choices'][0]['message']
                    # 直接メッセージ形式
                    return prediction

            return {"content": str(response)}
        except Exception as e:
            raise ValueError(f"Failed to extract message: {e}")
