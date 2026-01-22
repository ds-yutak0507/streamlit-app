import time
from typing import Any, Dict, List, Optional

import requests
from databricks.sdk import WorkspaceClient

class DatabricksServingChatClient:
    """
    Databricks Serving Endpoint (invocations) を叩くクライアント。
    - 認証ヘッダをキャッシュ
    - HTTP/APIエラーを分かりやすく
    - レスポンス形式の揺れに少し強く
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        endpoint_name: str,
        timeout_sec: int = 120,
        auth_cache_ttl_sec: int = 10 * 60,  # 10分キャッシュ
    ):
        self.w = workspace_client
        self.endpoint_name = endpoint_name
        self.timeout_sec = timeout_sec
        self.auth_cache_ttl_sec = auth_cache_ttl_sec

        self._cached_headers: Optional[Dict[str, str]] = None
        self._cached_at: float = 0.0

    def _invocations_url(self) -> str:
        host = self.w.config.host.rstrip("/")
        return f"{host}/serving-endpoints/{self.endpoint_name}/invocations"

    def _get_auth_headers_cached(self) -> Dict[str, str]:
        now = time.time()
        if self._cached_headers and (now - self._cached_at) < self.auth_cache_ttl_sec:
            return self._cached_headers

        headers = self.w.config.authenticate()
        if not headers or "Authorization" not in headers:
            raise RuntimeError(
                "Authentication headers are not available. "
                "Check App authentication / permissions."
            )
        self._cached_headers = dict(headers)
        self._cached_at = now
        return self._cached_headers

    def send_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ) -> Dict[str, Any]:
        if not self.endpoint_name:
            raise ValueError("ENDPOINT_NAME is not set.")

        url = self._invocations_url()
        headers = {
            **self._get_auth_headers_cached(),
            "Content-Type": "application/json",
        }

        payload = {
            "messages": messages,
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }

        r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_sec)

        # HTTPエラーを明示（Databricks側のエラーメッセージを見やすく）
        if not r.ok:
            # 可能ならJSONの詳細、ダメならテキスト
            detail = None
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"Serving invocation failed: HTTP {r.status_code}: {detail}")

        data = r.json()

        # Databricksやモデルの返却によっては error フィールド等が入ることがあるので拾う
        if isinstance(data, dict) and ("error" in data or "errors" in data):
            raise RuntimeError(f"Serving returned error: {data}")

        return data

    @staticmethod
    def extract_text(data: Dict[str, Any]) -> str:
        try:
            return data["choices"][0]["message"]["content"]
        except Exception as e:
            raise ValueError(f"Unexpected response format: {data}") from e
