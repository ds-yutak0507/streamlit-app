import json
import requests
import streamlit as st
from databricks.sdk import WorkspaceClient

# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="Simple Chat (Databricks Serving)", layout="centered")
st.title("Simple Chat (Databricks Serving)")

# ----------------------------
# Config
# ----------------------------
# Appsで環境変数として渡すのが正攻法
ENDPOINT_NAME = 'kikkawa-samplechat-model'

# Databricks client (Apps上では自動認証される想定)
w = WorkspaceClient()

def _get_auth_headers() -> dict:
    headers = w.config.authenticate()  # 例: {"Authorization": "Bearer ..."}
    if not headers or "Authorization" not in headers:
        raise RuntimeError(
            "Authentication headers are not available. "
            "Check App authentication / permissions."
        )
    return headers

def _invocations_url(endpoint_name: str) -> str:
    host = w.config.host.rstrip("/")
    # Serving Endpoint invocations API
    return f"{host}/serving-endpoints/{endpoint_name}/invocations"

def call_serving_chat(messages: list[dict], temperature: float, max_tokens: int) -> str:
    """
    まず chat 形式（messages）で投げる。
    """
    if not ENDPOINT_NAME:
        return "CHAT_ENDPOINT が未設定です（AppsのResourcesでServing endpointを追加するか、ENDPOINT_FALLBACKに直書きしてください）"

    url = _invocations_url(ENDPOINT_NAME)
    headers = {
        **_get_auth_headers(),
        "Content-Type": "application/json",
    }

    # 1) chat形式（多くの基盤モデル/プロバイダでこの形式）
    payload_chat = {
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }

    r = requests.post(url, headers=headers, json=payload_chat, timeout=120)

    return extract_text_from_response(r.json())

def extract_text_from_response(obj) -> str:
    """
    プロバイダ/endpointにより返却形式が違うので、よくある形を順に拾う。
    """
    # 返りが文字列ならそのまま
    if isinstance(obj, str):
        return obj

    # よくある OpenAI 互換: {"choices":[{"message":{"content":"..."}}]}
    try:
        return obj["choices"][0]["message"]["content"]
    except Exception:
        pass

    # {"choices":[{"text":"..."}]}
    try:
        return obj["choices"][0]["text"]
    except Exception:
        pass

    # {"candidates":[{"content":{"parts":[{"text":"..."}]}}]} (Gemini系でよくある)
    try:
        parts = obj["candidates"][0]["content"]["parts"]
        return "".join(p.get("text", "") for p in parts)
    except Exception:
        pass

    # {"output_text":"..."} など
    for key in ["output_text", "generated_text", "text", "response"]:
        if isinstance(obj, dict) and key in obj and isinstance(obj[key], str):
            return obj[key]

    # 最後はdump
    return json.dumps(obj, ensure_ascii=False, indent=2)

# ----------------------------
# Sidebar UI
# ----------------------------
with st.sidebar:
    st.header("Settings")
    st.write("CHAT_ENDPOINT =", ENDPOINT_NAME or "(not set)")
    system_prompt = st.text_area("System prompt", "You are a helpful assistant.", height=100)
    temperature = st.slider("temperature", 0.0, 1.0, 0.2, 0.05)
    max_tokens = st.slider("max_tokens", 64, 2048, 512, 64)

    debug = st.checkbox("Debug", value=False)

    if st.button("Clear chat"):
        st.session_state.messages = [{"role": "system", "content": system_prompt}]
        st.rerun()

# ----------------------------
# Chat state
# ----------------------------
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": system_prompt}]
else:
    # system promptは常に先頭に反映
    st.session_state.messages[0] = {"role": "system", "content": system_prompt}

# Show history (systemは表示しない)
for m in st.session_state.messages:
    if m["role"] == "system":
        continue
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

if debug:
    st.sidebar.subheader("Debug info")
    st.sidebar.json(st.session_state.messages[-6:])

# Input
prompt = st.chat_input("Type a message")
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                reply = call_serving_chat(
                    messages=st.session_state.messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as e:
                reply = f"Error: {e}"
        st.markdown(reply)

    st.session_state.messages.append({"role": "assistant", "content": reply})
