import streamlit as st
import requests
import pandas as pd
import json
from io import StringIO
import os

# ================= CONFIG =================
# 🔥 Smart backend URL handling (LOCAL + CLOUD)
if "BACKEND_URL" in st.secrets:
    FASTAPI_URL = st.secrets["BACKEND_URL"]
else:
    FASTAPI_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="AI-Powered Data Cleaning", layout="wide")

# Debug (optional)
st.sidebar.write(f"🔗 Backend: {FASTAPI_URL}")

# ================= SIDEBAR =================
st.sidebar.header("📊 Data Source Selection")

data_source = st.sidebar.radio(
    "Select Data Source:",
    ["CSV/Excel", "Database Query", "API Data"],
    index=0
)

# ================= TITLE =================
st.markdown("""
# 🚀 AI-Powered Data Cleaning
Clean your data effortlessly using AI-powered processing!
""")

# =========================================================
# 📁 CSV / EXCEL
# =========================================================
if data_source == "CSV/Excel":
    st.subheader("📂 Upload File for Cleaning")

    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=["csv", "xlsx"]
    )

    if uploaded_file is not None:
        try:
            file_ext = uploaded_file.name.split(".")[-1]

            if file_ext == "csv":
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            st.write("### 🔍 Raw Data Preview")
            st.dataframe(df)

        except Exception as e:
            st.error(f"❌ Error reading file: {e}")
            st.stop()

        if st.button("🚀 Clean Data"):
            try:
                files = {
                    "file": (uploaded_file.name, uploaded_file.getvalue())
                }

                response = requests.post(
                    f"{FASTAPI_URL}/clean-data",
                    files=files,
                    timeout=120
                )

                if response.status_code == 200:
                    st.subheader("🧪 Raw API Response")
                    st.json(response.json())

                    cleaned_data_raw = response.json().get("cleaned_data", [])

                    if isinstance(cleaned_data_raw, str):
                        cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw))
                    else:
                        cleaned_data = pd.DataFrame(cleaned_data_raw)

                    st.subheader("✅ Cleaned Data")
                    st.dataframe(cleaned_data)

                else:
                    st.error(f"❌ API Error: {response.text}")

            except requests.exceptions.RequestException as e:
                st.error(f"❌ Connection Error: {e}")


# =========================================================
# 🗄️ DATABASE
# =========================================================
elif data_source == "Database Query":
    st.subheader("🗄️ Enter Database Query")

    db_url = st.text_input(
        "Database Connection URL:",
        "postgresql://postgres:admin@localhost:5432/demodb"
    )

    query = st.text_area(
        "Enter SQL Query:",
        "SELECT * FROM my_table;"
    )

    if st.button("🚀 Fetch & Clean Data"):
        try:
            response = requests.post(
                f"{FASTAPI_URL}/clean-db",
                json={"db_url": db_url, "query": query},
                timeout=120
            )

            if response.status_code == 200:
                st.subheader("🧪 Raw API Response")
                st.json(response.json())

                cleaned_data_raw = response.json().get("cleaned_data", [])

                if isinstance(cleaned_data_raw, str):
                    cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw))
                else:
                    cleaned_data = pd.DataFrame(cleaned_data_raw)

                st.subheader("✅ Cleaned Data")
                st.dataframe(cleaned_data)

            else:
                st.error(f"❌ API Error: {response.text}")

        except requests.exceptions.RequestException as e:
            st.error(f"❌ Connection Error: {e}")


# =========================================================
# 🌐 API
# =========================================================
elif data_source == "API Data":
    st.subheader("🌐 Fetch Data from API")

    api_url = st.text_input(
        "Enter API Endpoint:",
        "https://jsonplaceholder.typicode.com/posts"
    )

    if st.button("🚀 Fetch & Clean Data"):
        try:
            response = requests.post(
                f"{FASTAPI_URL}/clean-api",
                json={"api_url": api_url},
                timeout=120
            )

            if response.status_code == 200:
                st.subheader("🧪 Raw API Response")
                st.json(response.json())

                cleaned_data_raw = response.json().get("cleaned_data", [])

                if isinstance(cleaned_data_raw, str):
                    cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw))
                else:
                    cleaned_data = pd.DataFrame(cleaned_data_raw)

                st.subheader("✅ Cleaned Data")
                st.dataframe(cleaned_data)

            else:
                st.error(f"❌ API Error: {response.text}")

        except requests.exceptions.RequestException as e:
            st.error(f"❌ Connection Error: {e}")


# =========================================================
# FOOTER
# =========================================================
st.markdown("""
---
🚀 Built with **Streamlit + FastAPI + AI (Groq)** for automated data cleaning 🔥
""")
