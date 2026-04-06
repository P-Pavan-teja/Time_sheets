import streamlit as st
import pandas as pd
import datetime as dt
from snowflake.snowpark.context import get_active_session

APPROVER_USER_ID = "TEJA"   # change this to your approver user id

st.set_page_config(page_title="Timesheet Approval", layout="wide")
st.title("Timesheet Approval Portal")

session = get_active_session()
current_user = st.user.user_name

st.write(f"Logged in as: **{current_user}**")

# Restrict access
if current_user.upper() != APPROVER_USER_ID.upper():
    st.error("You are not authorized to view the approval portal.")
    st.stop()

st.subheader("Pending approvals")

pending_df = session.sql("""
    SELECT
        TIMESHEET_ID,
        EMPLOYEE_EMAIL,
        ENTRY_DATE,
        PROJECT_NAME,
        HOURS_WORKED,
        STATUS,
        SUBMITTED_AT
    FROM TIMESHEETS
    WHERE STATUS = 'SUBMITTED'
    ORDER BY ENTRY_DATE, EMPLOYEE_EMAIL, PROJECT_NAME
""").to_pandas()

if pending_df.empty:
    st.info("No timesheets waiting for approval.")
    st.stop()

pending_df["Approve"] = False
pending_df["Reject"] = False
pending_df["Rejection_Reason"] = ""

edited = st.data_editor(
    pending_df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    column_config={
        "TIMESHEET_ID": st.column_config.NumberColumn("ID", disabled=True),
        "EMPLOYEE_EMAIL": st.column_config.TextColumn("Employee", disabled=True),
        "ENTRY_DATE": st.column_config.DateColumn("Date", disabled=True),
        "PROJECT_NAME": st.column_config.TextColumn("Project", disabled=True),
        "HOURS_WORKED": st.column_config.NumberColumn("Hours", disabled=True),
        "STATUS": st.column_config.TextColumn("Status", disabled=True),
        "SUBMITTED_AT": st.column_config.DatetimeColumn("Submitted at", disabled=True),
        "Approve": st.column_config.CheckboxColumn("Approve"),
        "Reject": st.column_config.CheckboxColumn("Reject"),
        "Rejection_Reason": st.column_config.TextColumn("Rejection reason"),
    },
    key="approval_editor",
)

conflict_rows = edited[(edited["Approve"]) & (edited["Reject"])]
if not conflict_rows.empty:
    st.error("A row cannot be both Approved and Rejected. Fix those rows first.")
    st.stop()

if st.button("Apply Decisions"):
    to_approve = edited[edited["Approve"]]
    to_reject = edited[edited["Reject"]]

    if to_approve.empty and to_reject.empty:
        st.warning("No rows selected for approval or rejection.")
    else:
        now = dt.datetime.utcnow()

        for _, row in to_approve.iterrows():
            session.sql("""
                UPDATE TIMESHEETS
                SET STATUS = 'APPROVED',
                    APPROVED_AT = ?,
                    APPROVED_BY = ?,
                    APPROVER_EMAIL = ?
                WHERE TIMESHEET_ID = ?
                  AND STATUS = 'SUBMITTED'
            """, params=[
                now,
                current_user,
                current_user,
                int(row["TIMESHEET_ID"]),
            ]).collect()

        for _, row in to_reject.iterrows():
            reason = (row.get("Rejection_Reason") or "").strip()
            session.sql("""
                UPDATE TIMESHEETS
                SET STATUS = 'REJECTED',
                    APPROVED_AT = ?,
                    APPROVED_BY = ?,
                    APPROVER_EMAIL = ?,
                    REJECTION_REASON = ?
                WHERE TIMESHEET_ID = ?
                  AND STATUS = 'SUBMITTED'
            """, params=[
                now,
                current_user,
                current_user,
                reason,
                int(row["TIMESHEET_ID"]),
            ]).collect()

        st.success(
            f"Processed {len(to_approve)} approval(s) and {len(to_reject)} rejection(s)."
        )
        st.rerun()
