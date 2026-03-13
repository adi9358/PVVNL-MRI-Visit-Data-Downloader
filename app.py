import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
import tempfile
import os

st.title("PVVNL MRI Visit Data Downloader")

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

        year_month = datetime.today().strftime("%Y%m")
        table_name = f"PVVNL5_SAIADM..METER_READING_TRANS_{year_month}"

        query = f"""
        SELECT mrt.*, mum.user_name
        FROM {table_name} mrt
        LEFT JOIN PVVNL5_SAIADM..MOBILE_USER_MST mum
        ON mrt.upload_by = mum.USER_ID
        """

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        file_path = temp_file.name
        temp_file.close()

        chunksize = 25000
        total_rows = 0

        excel_row_limit = 1048576
        sheet_number = 1
        current_row = 1

        progress = st.progress(0)

        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:

            workbook = writer.book
            worksheet = workbook.add_worksheet(f"Sheet{sheet_number}")
            writer.sheets[f"Sheet{sheet_number}"] = worksheet

            header_written = False

            for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunksize)):

                chunk = chunk.fillna("NULL")

                if not header_written:
                    for col_num, column in enumerate(chunk.columns):
                        worksheet.write(0, col_num, column)
                    header_written = True

                for _, row in chunk.iterrows():

                    # Create new sheet if Excel row limit reached
                    if current_row >= excel_row_limit:

                        sheet_number += 1
                        worksheet = workbook.add_worksheet(f"Sheet{sheet_number}")
                        writer.sheets[f"Sheet{sheet_number}"] = worksheet

                        for col_num, column in enumerate(chunk.columns):
                            worksheet.write(0, col_num, column)

                        current_row = 1

                    for col_idx, value in enumerate(row):

                        if isinstance(value, str) and value.startswith("http"):
                            worksheet.write_url(current_row, col_idx, value)
                        else:
                            worksheet.write(current_row, col_idx, value)

                    current_row += 1
                    total_rows += 1

                progress.progress(min((i + 1) * 5, 100))

        conn.close()

        st.success(f"Total Records Fetched: {total_rows}")

        today = datetime.today().strftime("%d-%m-%Y")
        file_name = f"PVVNL 5 KW TO BELOW 10 KW MRI VISIT DATA AS ON DATE {today}.xlsx"

        with open(file_path, "rb") as f:

            st.download_button(
                label="Download Excel",
                data=f,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        os.remove(file_path)

    except Exception as e:
        st.error(str(e))
