import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import io

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

    try:

        # -----------------------------
        # SQLAlchemy Engine
        # -----------------------------
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )

        engine = create_engine(connection_string)

        # -----------------------------
        # Current Month Table
        # -----------------------------
        year_month = datetime.today().strftime("%Y%m")

        table_name = f"PVVNL5_SAIADM..METER_READING_TRANS_{year_month}"

        query = f"""
        SELECT mrt.*, mum.user_name
        FROM {table_name} mrt
        LEFT JOIN PVVNL5_SAIADM..MOBILE_USER_MST mum
        ON mrt.upload_by = mum.USER_ID
        """

        df = pd.read_sql(query, engine)

        # Replace NaN with NULL
        df = df.fillna("NULL")

        st.success(f"Total Records: {len(df)}")

        # -----------------------------
        # Excel File Name
        # -----------------------------
        today = datetime.today().strftime("%d-%m-%Y")

        file_name = f"PVVNL 5 KW TO BELOW 10 KW MRI VISIT DATA AS ON DATE {today}.xlsx"

        # -----------------------------
        # Write Excel
        # -----------------------------
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

        output.seek(0)

        st.download_button(
            label="Download Excel",
            data=output,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(e)
