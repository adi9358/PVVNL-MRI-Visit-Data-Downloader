import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
import tempfile
import os

st.title("PVVNL MRI Visit Data Downloader")

# -----------------------------
# USER INPUT
# -----------------------------
server = "4.188.235.99,8225"
database = "PVVNL5_SAIADM"

username = st.text_input("SQL Username", value="Avneesh")
password = st.text_input("SQL Password", type="password")

fetch = st.button("Fetch Data")

if fetch:

    if not username or not password:
        st.warning("Please enter username and password")
        st.stop()

    try:

        # -----------------------------
        # SQL CONNECTION
        # -----------------------------
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER=tcp:{server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )

        st.success("Database connected successfully")

        # -----------------------------
        # CURRENT MONTH TABLE
        # -----------------------------
        year_month = datetime.today().strftime("%Y%m")

        table_name = f"PVVNL5_SAIADM..METER_READING_TRANS_{year_month}"

        query = f"""
        SELECT mrt.*, mum.user_name
        FROM {table_name} mrt
        LEFT JOIN PVVNL5_SAIADM..MOBILE_USER_MST mum
        ON mrt.upload_by = mum.USER_ID
        """

        # -----------------------------
        # CREATE TEMP CSV FILE
        # -----------------------------
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        file_path = temp_file.name
        temp_file.close()

        chunksize = 15000
        total_rows = 0

        progress = st.progress(0)

        first_chunk = True

        with open(file_path, "w", newline="", encoding="utf-8") as f:

            for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunksize)):

                chunk = chunk.fillna("NULL")

                chunk.to_csv(
                    f,
                    index=False,
                    header=first_chunk
                )

                first_chunk = False
                total_rows += len(chunk)

                progress.progress(min((i + 1) * 10, 100))

        conn.close()

        st.success(f"Total Records Fetched: {total_rows}")

        # -----------------------------
        # FILE NAME
        # -----------------------------
        today = datetime.today().strftime("%d-%m-%Y")

        file_name = f"PVVNL 5 KW TO BELOW 10 KW MRI VISIT DATA AS ON DATE {today}.csv"

        # -----------------------------
        # DOWNLOAD BUTTON
        # -----------------------------
        with open(file_path, "rb") as f:

            st.download_button(
                label="Download CSV",
                data=f,
                file_name=file_name,
                mime="text/csv"
            )

        # optional cleanup
        os.remove(file_path)

    except pyodbc.OperationalError as e:

        st.error("❌ Database connection failed")
        st.error(str(e))

    except Exception as e:

        st.error("❌ Unexpected error occurred")
        st.error(str(e))
