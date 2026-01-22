import streamlit as st
from databricks.sdk import WorkspaceClient

from dbx_serving_client import DatabricksServingChatClient

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

# Chat Client
client = DatabricksServingChatClient(w, ENDPOINT_NAME)

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
                data = client.send_chat(
                    messages=st.session_state.messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                reply = client.extract_text(data)

                if debug:
                    st.sidebar.subheader("Last response (raw)")
                    st.sidebar.json(data)

            except Exception as e:
                reply = f"Error: {e}"

        st.markdown(reply)

    st.session_state.messages.append({"role": "assistant", "content": reply})
