import streamlit as st
from databricks.sdk import WorkspaceClient

from dbx_serving_client import DatabricksServingChatClient
from unity_catalog_tools import UnityCatalogClient

# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="Simple Chat (Databricks Serving)", layout="centered")
st.title("Simple Chat (Databricks Serving)")

# ----------------------------
# Config
# ----------------------------
ENDPOINT_NAME = "kikkawa-samplechat-model"

# Databricks client (Apps上では自動認証される想定)
w = WorkspaceClient()

# Unity Catalog client
uc_client = UnityCatalogClient(w)

# Chat Client
client = DatabricksServingChatClient(w, ENDPOINT_NAME)

# テーブル情報を取得してキャッシュ
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_table_info():
    """Unity Catalogからテーブル情報を取得してフォーマット"""
    try:
        # テーブル一覧を取得
        tables = uc_client.list_tables("yuta_kikkawa", "demo_sales")

        if not tables:
            return "スキーマ yuta_kikkawa.demo_sales にテーブルが見つかりませんでした。"

        # テーブル情報を整形
        table_info = "## Available Tables in yuta_kikkawa.demo_sales\n\n"

        for table in tables:
            # 各テーブルの詳細を取得
            try:
                details = uc_client.get_table_details("yuta_kikkawa", "demo_sales", table["name"])

                table_info += f"### {table['name']}\n"
                table_info += f"Type: {details['table_type']}\n"
                if details['comment']:
                    table_info += f"Description: {details['comment']}\n"
                table_info += "\nColumns:\n"

                for col in details['columns']:
                    table_info += f"- {col['name']} ({col['type']})"
                    if col['comment']:
                        table_info += f": {col['comment']}"
                    table_info += "\n"

                table_info += "\n"
            except Exception as e:
                table_info += f"- {table['name']}: 詳細情報の取得に失敗\n\n"

        return table_info
    except Exception as e:
        return f"テーブル情報の取得中にエラーが発生しました: {str(e)}"

# ----------------------------
# Sidebar UI
# ----------------------------
with st.sidebar:
    st.header("Settings")
    st.write("CHAT_ENDPOINT =", ENDPOINT_NAME or "(not set)")

    # テーブル情報を取得
    table_info = get_table_info()

    default_system_prompt = f"""You are a helpful assistant with knowledge of Unity Catalog tables.

When users ask about table information, use the following schema information:

{table_info}

Always provide clear, well-formatted responses about table structure and metadata."""

    system_prompt = st.text_area("System prompt", default_system_prompt, height=300)
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
                reply = client.send_chat(
                    messages=st.session_state.messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as e:
                reply = f"Error: {e}"

        st.markdown(reply)

    st.session_state.messages.append({"role": "assistant", "content": reply})
