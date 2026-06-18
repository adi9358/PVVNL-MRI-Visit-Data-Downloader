
import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
import tempfile
import os

st.set_page_config(page_title="PVVNL Data Downloader", layout="wide")

st.title("PVVNL Data Downloader")

# =========================
# DATABASE CONFIG
# =========================
server = "20.244.132.226"
database = "PVVNL5_SAIADM"

username = st.text_input("SQL Username", value="avneesh1")
password = st.text_input("SQL Password", type="password")

# =========================
# REPORT SELECTION
# =========================
report_type = st.selectbox(
    "Select Report",
    [
        "MRI Visit Data",
        "Bill Distribution Data"
    ]
)

# =========================
# MONTH SELECTION FOR BILL DISTRIBUTION
# =========================
if report_type == "Bill Distribution Data":

    months = [
        "January", "February", "March", "April",
        "May", "June", "July", "August",
        "September", "October", "November", "December"
    ]

    col1, col2 = st.columns(2)

    with col1:
        selected_month = st.selectbox(
            "Select Month",
            range(1, 13),
            index=datetime.today().month - 1,
            format_func=lambda x: months[x - 1]
        )

    with col2:
        selected_year = st.selectbox(
            "Select Year",
            range(2024, datetime.today().year + 1),
            index=len(range(2024, datetime.today().year + 1)) - 1
        )

    year_month = f"{selected_year}{selected_month:02d}"   # 202506
    month_year = f"{selected_month:02d}{selected_year}"   # 062025

else:

    year_month = datetime.today().strftime("%Y%m")
    month_year = datetime.today().strftime("%m%Y")

# =========================
# FETCH BUTTON
# =========================
fetch = st.button("Fetch Data")

if fetch:

    if not username or not password:
        st.warning("Please enter username and password")
        st.stop()

    try:

        # =========================
        # DATABASE CONNECTION
        # =========================
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

        today = datetime.today().strftime("%d-%m-%Y")

        # =========================
        # MRI VISIT DATA QUERY
        # =========================
        if report_type == "MRI Visit Data":

            query = f"""
            SELECT
                MRT.*,
                MUM.user_name
            FROM PVVNL5_SAIADM..METER_READING_TRANS_{year_month} MRT
            LEFT JOIN PVVNL5_SAIADM..MOBILE_USER_MST MUM
                ON MRT.upload_by = MUM.USER_ID
            """

            file_name = (
                f"PVVNL MRI VISIT DATA AS ON DATE {today}.xlsx"
            )

        # =========================
        # BILL DISTRIBUTION QUERY
        # =========================
        else:

            query = f"""
            SELECT
                MRT.*,
                MUM.user_name,
                ZONE,
                CIRCLE,
                CM.DIV_NAME
            FROM PVVNL5_SAIADM..BILL_DISTRIBUTION_{year_month} MRT
            LEFT JOIN PVVNL5_SAIADM..MOBILE_USER_MST MUM
                ON MRT.upload_by = MUM.USER_ID
            LEFT JOIN
            (
                SELECT AC_ID, DIV_NAME
                FROM PVVNL5_MRI_DATA..CONSUMER_MASTER_{month_year}
            ) CM
                ON MRT.AC_ID = CM.AC_ID
            """

            file_name = (
                f"PVVNL 5 KW to Below 10 KW BILL DISTRIBUTION DATA {months[selected_month-1]} "
                f"{selected_year}.xlsx"
            )

        # =========================
        # TEMP EXCEL FILE
        # =========================
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".xlsx"
        )

        file_path = temp_file.name
        temp_file.close()

        chunksize = 20000
        start_row = 0
        total_rows = 0

        progress_bar = st.progress(0)
        status = st.empty()

        # =========================
        # EXPORT TO EXCEL IN CHUNKS
        # =========================
        with pd.ExcelWriter(
            file_path,
            engine="xlsxwriter",
            engine_kwargs={
                "options": {
                    "strings_to_urls": False
                }
            }
        ) as writer:

            for chunk in pd.read_sql_query(
                query,
                conn,
                chunksize=chunksize
            ):

                chunk = chunk.fillna("NULL")

                chunk.to_excel(
                    writer,
                    sheet_name="Data",
                    startrow=start_row,
                    index=False,
                    header=(start_row == 0)
                )

                start_row += len(chunk)
                total_rows += len(chunk)

                status.text(
                    f"Records Processed : {total_rows:,}"
                )

                progress_bar.progress(
                    min(total_rows / 300000, 1.0)
                )

        conn.close()

        progress_bar.progress(1.0)

        st.success(
            f"Total Records Fetched : {total_rows:,}"
        )

        # =========================
        # DOWNLOAD BUTTON
        # =========================
        with open(file_path, "rb") as f:

            st.download_button(
                label="📥 Download Excel",
                data=f.read(),
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        # =========================
        # DELETE TEMP FILE
        # =========================
        try:
            os.remove(file_path)
        except:
            pass

    except Exception as e:

        st.error("❌ Error occurred")
        st.error(str(e))

