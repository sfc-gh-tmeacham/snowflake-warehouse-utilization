import streamlit as st
import st_connection
import st_connection.snowflake

try:
    st.set_page_config(
        page_title="Snowflake Warehouse Utilization",
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
except:
    pass


def main():
    pass


if __name__ == "__main__":
    try:

        main()

        st.title("Snowflake Warehouse Utilization")
        # Things above here will be run before (and after) you log in.
        if 'session' not in locals():
            with st.expander("Login Help", False):
                st.markdown(
                """***account***: this should be the portion https://<account>.snowflakecomputing.com
                    
    ***database***: This should remain ***SNOWFLAKE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location

    ***schema***: This should remain ***ACCOUNT_USAGE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location

    ***role***: This should remain ***ACCOUNTADMIN*** unless you have delegated access to `query_history` and `warehouse_metering_history`
        """)
        session = st.connection.snowflake.login({
            'account': 'XXX',
            'user': '',
            'password': None,
            'warehouse': 'ADHOC_WH',
            'database': 'SNOWFLAKE',
            'schema': 'ACCOUNT_USAGE',
            'role': 'ACCOUNTADMIN',
        }, {
            'ttl': 120
        }, 'Snowflake Login')


        # Nothing below here will be run until you log in.

        df = session.sql("""SELECT 
            WAREHOUSE_NAME,
            WAREHOUSE_SIZE,
            SUM(NO_OF_QUERIES) AS NUM_QUERIES,
            SUM(TOTAL_ELAPSED_TIME) AS TOTAL_ELAPSED_TIME_MS,
            SUM(EXPECTED_CREDITS) AS EXPECTED_CREDITS,
            SUM(CREDITS_USED) AS ACTUAL_CREDITS
        FROM  (
            SELECT
                TO_VARCHAR(Q.START_TIME, 'YYYY-MM-DD HH:00:00') AS DATETIME,
                Q.WAREHOUSE_NAME,
                Q.WAREHOUSE_SIZE,
                AVG(Q.CLUSTER_NUMBER) AS CLUSTER_NUMBER,
                COUNT(*) AS NO_OF_QUERIES,
                SUM(TOTAL_ELAPSED_TIME) AS TOTAL_ELAPSED_TIME,
                SUM(
                    TOTAL_ELAPSED_TIME / 1000 / 60 / 60 *
                    CASE WAREHOUSE_SIZE
                        WHEN 'X-Small' THEN 1
                        WHEN 'Small'  THEN 2
                        WHEN 'Medium' THEN 4
                        WHEN 'Large'  THEN 8
                        WHEN 'X-Large' THEN 16
                        WHEN '2X-Large' THEN 32
                        WHEN '3X-Large' THEN 64
                        WHEN '4X-Large' THEN 128
                        WHEN '5X-Large' THEN 256
                        WHEN '6X-Large' THEN 512
                        ELSE 0
                    END
                ) AS EXPECTED_CREDITS,
                MAX(CREDITS_USED) AS CREDITS_USED
            FROM QUERY_HISTORY Q
            LEFT JOIN WAREHOUSE_METERING_HISTORY M ON M.WAREHOUSE_ID = Q.WAREHOUSE_ID AND TO_VARCHAR(Q.START_TIME, 'YYYY-MM-DD HH:00:00')::TIMESTAMP=M.START_TIME
            WHERE 1=1
                AND WAREHOUSE_SIZE IS NOT NULL
                AND Q.START_TIME >= DATEADD(MONTH, -1, CURRENT_DATE)
            GROUP BY 1,2,3
        ) A 
        GROUP BY 1,2
        ORDER BY 1 ASC
        """).to_pandas()
        # df = df.collect()
        df = df.fillna(0)
        df = df[df['ACTUAL_CREDITS'] != 0]
        df["UTILIZATION"] = (df["EXPECTED_CREDITS"] /
                            df['ACTUAL_CREDITS'])
        df["TOTAL_QUERY_S"] = df["TOTAL_ELAPSED_TIME_MS"]/1000
        df["TOTAL_QUERY_MIN"] = df["TOTAL_ELAPSED_TIME_MS"]/1000/60
        df["TOTAL_QUERY_HRS"] = df["TOTAL_ELAPSED_TIME_MS"]/1000/60/60
        df["AVG_QUERY_TIME_MS"] = df['TOTAL_ELAPSED_TIME_MS'] / df["NUM_QUERIES"]
        df["AVG_QUERY_TIME_S"] = df["AVG_QUERY_TIME_MS"] / 1000
        df["AVG_QUERY_CREDITS"] = df['ACTUAL_CREDITS'] / df["NUM_QUERIES"]

        df['WAREHOUSE_CPH'] = 0
        df.loc[df['WAREHOUSE_SIZE'] == 'X-Small', 'WAREHOUSE_CPH'] = 1
        df.loc[df['WAREHOUSE_SIZE'] == 'Small', 'WAREHOUSE_CPH'] = 2
        df.loc[df['WAREHOUSE_SIZE'] == 'Medium', 'WAREHOUSE_CPH'] = 4
        df.loc[df['WAREHOUSE_SIZE'] == 'Large', 'WAREHOUSE_CPH'] = 8
        df.loc[df['WAREHOUSE_SIZE'] == 'X-Large', 'WAREHOUSE_CPH'] = 16
        df.loc[df['WAREHOUSE_SIZE'] == '2X-Large', 'WAREHOUSE_CPH'] = 32
        df.loc[df['WAREHOUSE_SIZE'] == '3X-Large', 'WAREHOUSE_CPH'] = 64
        df.loc[df['WAREHOUSE_SIZE'] == '4X-Large', 'WAREHOUSE_CPH'] = 128
        df.loc[df['WAREHOUSE_SIZE'] == '5X-Large', 'WAREHOUSE_CPH'] = 256
        df.loc[df['WAREHOUSE_SIZE'] == '6X-Large', 'WAREHOUSE_CPH'] = 512
        df["TOTAL_WH_HRS"] = df['ACTUAL_CREDITS'] / df["WAREHOUSE_CPH"]
        df["TOTAL_WH_MIN"] = df["TOTAL_WH_HRS"] * 60
        df["TOTAL_WH_S"] = df["TOTAL_WH_MIN"] * 60

        st.caption("Shows Estimated Credit Usage computed from QUERY_HISTORY vs Actual Credit Usage from WAREHOUSE_METERING_HISTORY over the past month")
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric(label="Number of Queries",
                    value='{:,.0f}'.format(df["NUM_QUERIES"].sum()))
        col2.metric(label="Total Query Hrs",
                    value='{:,.0f}'.format(df["TOTAL_QUERY_HRS"].sum()))
        col3.metric(label="Avg Query Time (s)",
                    value='{:,.2f}'.format(df["TOTAL_ELAPSED_TIME_MS"].mean()/1000))
        col4.metric(label="Credits Estimate",
                    value='{:,.0f}'.format(df["EXPECTED_CREDITS"].sum()))
        col5.metric(label="Actual Credits Used", value='{:,.0f}'.format(df["ACTUAL_CREDITS"].sum(
        )))
        col6.metric(label="Utilization %", value='{:,.0%}'.format(
            df["EXPECTED_CREDITS"].sum()/df["ACTUAL_CREDITS"].sum()), help="Utilization of Warehouses")

        st.write(df[[
                    'WAREHOUSE_NAME',
                    'WAREHOUSE_SIZE',
                    # 'AUTOSUSPEND_TIME',
                    'NUM_QUERIES',
                    'EXPECTED_CREDITS',
                    'ACTUAL_CREDITS',
                    'UTILIZATION',
                    'TOTAL_QUERY_HRS',
                    'TOTAL_WH_HRS',
                    'AVG_QUERY_CREDITS',
                    'AVG_QUERY_TIME_S',
                    # 'MIN_CLUSTER_COUNT',
                    # 'MAX_CLUSTER_COUNT',
                    # 'AVG_CLUSTER_SIZE',
                    # 'ENABLE_QUERY_ACCELERATION'
                ]].sort_values(by=['UTILIZATION'], ascending=False).style.background_gradient(axis=0).format({
                    'NUM_QUERIES': '{:,.0f}',
                    'TOTAL_ELAPSED_TIME': '{:,.0f}',
                    'EXPECTED_CREDITS': '{:,.2f}',
                    'CREDITS_USED': '{:,.0f}',
                    'UTILIZATION': '{:,.0%}',
                    'TOTAL_ELAPSED_TIME_MS': '{:,.0f}',
                    'TOTAL_QUERY_HRS': '{:,.2f}',
                    'TOTAL_WH_HRS': '{:,.2f}',
                    'MIN_CLUSTER_COUNT': '{:,.0f}',
                    'MAX_CLUSTER_COUNT': '{:,.0f}',
                    'AUTOSUSPEND_TIME': '{:,.0f}',
                    'AVG_CLUSTER_SIZE': '{:,.2f}',
                    'ACTUAL_CREDITS': '{:,.2f}',
                    'AVG_QUERY_TIME_S': '{:,.3f}',
                    'AVG_QUERY_CREDITS': '{:,.2f}',
                }))
        st.write('{:,.0f} records'.format(
                    len(df)))
        # st.dataframe(df)

        with st.expander("How To Use This Dashboard", False):
            st.markdown("""[Snowflake costs](https://docs.snowflake.com/en/user-guide/admin-usage-billing.html) are primarily based on usage of data storage and the number of virtual warehouses you use, how long they run, and their size. 
            
Because warehouses continue to bill even when idle, a calculation can be performed to identify the Warehouse Utilization by comparing the time spent querying (as calculated from the `query_history` view) versus the actual credits billed (as provided by `warehouse_metering_history`).

A 100% utilization would be obtained by always running sequential queries and then automatically suspending immediately after execution. However, some scenarios would cause a utilization less than 100% such as:
* Auto Suspend Periods
* The 60 second warehouse uptime minimum.

A utilization of >100% could be obtained by running queries concurrently. """)
            st.image('Utilization_Scenarios.png')

            st.markdown("""A low utilization does is not implicitly a bad thing, but could indicate there are opportunities to optimize. Some techniques may include:

* Reducing the auto suspend period of a warehouse by running `ALTER WAREHOUSE <WAREHOUSE NAME> SET AUTO_SUSPEND=60`

* Consolidating warehousing to leverage the uptime of other like sized warehouses

For more information and suggestions on how to optimize your Snowflake environment, reach out to your Snowflake Account team!""")
    except Exception as e:
        st.write(e)
