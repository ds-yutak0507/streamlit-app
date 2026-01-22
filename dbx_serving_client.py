from typing import List, Dict
from databricks.sdk import WorkspaceClient
from openai import OpenAI

class DatabricksServingChatClient:
    """
    Databricks OpenAI クライアントで Serving Endpoint を呼ぶクライアント。
    """
    def __init__(self, workspace_client: WorkspaceClient, endpoint_name: str):
        self.w = workspace_client
        self.endpoint_name = endpoint_name

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
