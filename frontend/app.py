import streamlit as st
import requests
import pandas as pd
import io

st.set_page_config(page_title="🧠 DataSmith", page_icon="⚙️", layout="centered")

st.title("🧠 DataSmith – Smart Data Cleaner & Labeler")
st.caption("Upload your dataset and let AI do the cleaning & labeling for you!")

# File upload
uploaded_file = st.file_uploader("📤 Upload CSV file", type=["csv"])

if uploaded_file:
    with st.spinner("Uploading and processing... ⏳"):
        files = {"file": uploaded_file.getvalue()}
        res = requests.post("http://127.0.0.1:8000/process/", files={"file": uploaded_file})

        if res.status_code == 200:
            data = res.json()
            st.success(data["message"])

            # Show summary
            st.write("✅ Rows processed:", data["rows_processed"])

            # Read the labeled CSV
            labeled_path = data["labeled_file"]
            df = pd.read_csv(labeled_path)

            st.subheader("📊 Cleaned & Labeled Data Preview")
            st.dataframe(df.head())

            # Download button
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            st.download_button(
                label="⬇️ Download Labeled CSV",
                data=csv_buffer.getvalue(),
                file_name="labeled_output.csv",
                mime="text/csv",
            )
        else:
            st.error("⚠️ Error: Could not process the file. Check if backend is running.")
