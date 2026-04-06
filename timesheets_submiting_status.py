import streamlit as st
import pandas as pd
import datetime as dt
from snowflake.snowpark.context import get_active_session

# ---------------------------
# Helper: Friday to Thursday week
# ---------------------------
def get_friday_thursday_week(input_date):
    days_since_friday = (input_date.weekday() - 4) % 7
    week_start = input_date - dt.timedelta(days=days_since_friday)
    week_end = week_start + dt.timedelta(days=6)
    return week_start, week_end

def normalize_date_range(date_value, default_start, default_end):
    if isinstance(date_value, (tuple, list)):
        if len(date_value) == 2 and date_value[0] and date_value[1]:
            return date_value[0], date_value[1]
        if len(date_value) >= 1 and date_value[0]:
            return get_friday_thursday_week(date_value[0])
        return default_start, default_end
    return get_friday_thursday_week(date_value)

# ---------------------------
# Page setup
# ---------------------------
st.set_page_config(page_title="Weekly Timesheet", layout="wide")

session = get_active_session()
user_email = st.user.user_name

page = st.sidebar.radio(
    "Navigation",
    ["Enter Weekly Hours", "My Approval Status"]
)

# =========================================================
# PAGE 1 - ENTER WEEKLY HOURS
# =========================================================
if page == "Enter Weekly Hours":
    st.title("Timesheet Calculator")
    st.write(f"Logged in as: **{user_email}**")

    st.subheader("Select week")

    today = dt.date.today()
    default_start, default_end = get_friday_thursday_week(today)

    selected_date = st.date_input(
        "Pick any date in the week",
        value=default_start,
        format="YYYY-MM-DD",
        key="week_start_input"
    )

    week_start, week_end = get_friday_thursday_week(selected_date)
    week_days = [week_start + dt.timedelta(days=i) for i in range(7)]

    day_labels = [d.strftime("%a\n%m/%d") for d in week_days]
    day_col_names = [d.strftime("%Y-%m-%d") for d in week_days]

    st.caption(f"Week: {week_start} to {week_end}")

    # ---------------------------
    # Load active projects
    # ---------------------------
    projects_df = session.sql("""
        SELECT PROJECT_NAME
        FROM PROJECT_APPROVER_MAP
        WHERE IS_ACTIVE = TRUE
        ORDER BY PROJECT_NAME
    """).to_pandas()

    if projects_df.empty:
        st.error("No active projects found in PROJECT_APPROVER_MAP.")
        st.stop()

    project_list = projects_df["PROJECT_NAME"].tolist()

    # ---------------------------
    # Build input grid
    # ---------------------------
    base = {"Project": project_list}
    for col in day_col_names:
        base[col] = [0.0] * len(project_list)

    df = pd.DataFrame(base)

    st.subheader("Enter Weekly Hours")

    column_config = {
        "Project": st.column_config.TextColumn("Investment / Project", disabled=True)
    }

    for label, col in zip(day_labels, day_col_names):
        column_config[col] = st.column_config.NumberColumn(
            label,
            min_value=0.0,
            max_value=24.0,
            step=0.5,
        )

    edited_df = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config=column_config,
        key="week_editor",
    )

    for col in day_col_names:
        edited_df[col] = pd.to_numeric(edited_df[col], errors="coerce").fillna(0.0)

    week_total = float(edited_df[day_col_names].sum().sum())
    st.markdown(f"### Week Total Hours: {week_total:.2f}")

    # ---------------------------
    # Submit with MERGE (no duplication)
    # ---------------------------
    if st.button("Submit Timesheet"):
        df_submit = edited_df.copy()

        non_zero = []
        for _, row in df_submit.iterrows():
            project = row["Project"]
            for day, col in zip(week_days, day_col_names):
                hours = float(row[col])
                if hours > 0:
                    non_zero.append((project, day, hours))

        if not non_zero:
            st.warning("No hours entered. Please fill at least one cell greater than 0.")
        else:
            processed_rows = 0

            for project, day, hours in non_zero:
                session.sql("""
                    MERGE INTO TIMESHEETS tgt
                    USING (
                        SELECT
                            ? AS EMPLOYEE_EMAIL,
                            ? AS ENTRY_DATE,
                            ? AS PROJECT_NAME,
                            ? AS HOURS_WORKED
                    ) src
                    ON tgt.EMPLOYEE_EMAIL = src.EMPLOYEE_EMAIL
                       AND tgt.ENTRY_DATE = src.ENTRY_DATE
                       AND tgt.PROJECT_NAME = src.PROJECT_NAME
                    WHEN MATCHED THEN
                        UPDATE SET
                            tgt.HOURS_WORKED = src.HOURS_WORKED,
                            tgt.STATUS = 'SUBMITTED',
                            tgt.SUBMITTED_AT = CURRENT_TIMESTAMP(),
                            tgt.APPROVED_AT = NULL,
                            tgt.APPROVED_BY = NULL,
                            tgt.APPROVER_EMAIL = NULL,
                            tgt.REJECTION_REASON = NULL
                    WHEN NOT MATCHED THEN
                        INSERT (
                            EMPLOYEE_EMAIL,
                            ENTRY_DATE,
                            PROJECT_NAME,
                            HOURS_WORKED,
                            STATUS,
                            SUBMITTED_AT,
                            APPROVED_AT,
                            APPROVED_BY,
                            APPROVER_EMAIL,
                            REJECTION_REASON
                        )
                        VALUES (
                            src.EMPLOYEE_EMAIL,
                            src.ENTRY_DATE,
                            src.PROJECT_NAME,
                            src.HOURS_WORKED,
                            'SUBMITTED',
                            CURRENT_TIMESTAMP(),
                            NULL,
                            NULL,
                            NULL,
                            NULL
                        )
                """, params=[
                    user_email,
                    day,
                    project,
                    hours,
                ]).collect()

                processed_rows += 1

            st.success(f"Processed {processed_rows} timesheet row(s) with no duplication.")

    # ---------------------------
    # Weekly summary
    # ---------------------------
    st.subheader("My Timesheets (selected week)")

    my_ts = session.sql("""
        SELECT
            TIMESHEET_ID,
            ENTRY_DATE,
            PROJECT_NAME,
            HOURS_WORKED,
            STATUS,
            SUBMITTED_AT
        FROM TIMESHEETS
        WHERE EMPLOYEE_EMAIL = ?
          AND ENTRY_DATE BETWEEN ? AND ?
        ORDER BY ENTRY_DATE, PROJECT_NAME
    """, params=[user_email, week_start, week_end]).to_pandas()

    if my_ts.empty:
        st.info("No timesheets submitted for this selected week yet.")
    else:
        st.dataframe(my_ts, use_container_width=True)

# =========================================================
# PAGE 2 - MY APPROVAL STATUS
# =========================================================
elif page == "My Approval Status":
    st.title("My Approval Status")
    st.write(f"Logged in as: **{user_email}**")

    today = dt.date.today()
    default_start, default_end = get_friday_thursday_week(today)

    status_date_range = st.date_input(
        "Select date range",
        value=(default_start, default_end),
        format="YYYY-MM-DD",
        key="status_date_range"
    )

    from_date, to_date = normalize_date_range(
        status_date_range,
        default_start,
        default_end
    )

    st.caption(f"Showing records from {from_date} to {to_date}")

    status_filter = st.selectbox(
        "Filter by status",
        ["ALL", "SUBMITTED", "APPROVED", "REJECTED"],
        index=0
    )

    base_query = """
        SELECT
            TIMESHEET_ID,
            ENTRY_DATE,
            PROJECT_NAME,
            HOURS_WORKED,
            STATUS,
            APPROVED_BY,
            APPROVER_EMAIL,
            APPROVED_AT,
            REJECTION_REASON,
            SUBMITTED_AT
        FROM TIMESHEETS
        WHERE EMPLOYEE_EMAIL = ?
          AND ENTRY_DATE BETWEEN ? AND ?
    """

    params = [user_email, from_date, to_date]

    if status_filter != "ALL":
        base_query += " AND STATUS = ?"
        params.append(status_filter)

    base_query += " ORDER BY ENTRY_DATE DESC, SUBMITTED_AT DESC"

    status_df = session.sql(base_query, params=params).to_pandas()

    if status_df.empty:
        st.info("No timesheet records found for the selected date range and filter.")
    else:
        st.dataframe(status_df, use_container_width=True)

    st.subheader("Status Summary")

    summary_query = """
        SELECT
            STATUS,
            COUNT(*) AS RECORD_COUNT,
            SUM(HOURS_WORKED) AS TOTAL_HOURS
        FROM TIMESHEETS
        WHERE EMPLOYEE_EMAIL = ?
          AND ENTRY_DATE BETWEEN ? AND ?
    """

    summary_params = [user_email, from_date, to_date]

    if status_filter != "ALL":
        summary_query += " AND STATUS = ?"
        summary_params.append(status_filter)

    summary_query += " GROUP BY STATUS ORDER BY STATUS"

    summary_df = session.sql(summary_query, params=summary_params).to_pandas()

    if summary_df.empty:
        st.info("No summary available for the selected range.")
    else:
        st.dataframe(summary_df, use_container_width=True)
