import streamlit as st
import st_connection
import st_connection.snowflake
from snowflake.snowpark.functions import avg, sum, col, lit, datediff, dateadd
import pandas as pd
import datetime

try:
    st.set_page_config(
        page_title="Snowflake Warehouse Utilization",
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
except:
    pass

try:
    # Hack to make tooltips work in full screen
    # https://discuss.streamlit.io/t/tool-tips-in-fullscreen-mode-for-charts/6800/7
    st.markdown('<style>#vg-tooltip-element{z-index: 1000051}</style>',
                unsafe_allow_html=True)
    st.markdown('<style>.vega-embed, canvas.marks{width:100% !important}</style>',
                unsafe_allow_html=True)
    # st.markdown('<style>.vega-embed .chart-wrapper canvas{width:100% !important}</style>',
    #             unsafe_allow_html=True)
    pass
except:
    pass

disclaimer = """Disclaimer: Use at your own discretion. This site does not store your Snowflake credentials and your credentials are only used as a passthrough to connect to your Snowflake account."""

def main():
    pass


if __name__ == "__main__":
    try:

        main()

        st.title("Snowflake Warehouse Utilization")
        # Things above here will be run before (and after) you log in.
        if 'ST_SNOW_SESS' not in st.session_state:
            with st.expander("Login Help", False):
                st.markdown(
                """
***account***: this should be the portion in between "https://" and ".snowflakecomputing.com" - for example https://<account>.snowflakecomputing.com
                    
***database***: This should remain ***SNOWFLAKE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location

***schema***: This should remain ***ACCOUNT_USAGE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location

***role***: This should remain ***ACCOUNTADMIN*** unless you have delegated access to `query_history` and `warehouse_metering_history`
        """)
            st.caption(disclaimer)

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

        st.caption("Shows Estimated Credit Usage computed from QUERY_HISTORY vs Actual Credit Usage from WAREHOUSE_METERING_HISTORY")

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

        col1, col2, col3 = st.columns(3)
        try:
            with col3:
                start_date, end_date = st.date_input(
                    'start date  - end date :',
                    value=[datetime.date.today() + datetime.timedelta(days=-31), datetime.date.today()],
                    max_value=datetime.date.today()
                )
                if start_date < end_date:
                    pass
                else:
                    st.error('Error: End date must fall after start date.')
                
        except:
            st.error("Please select an end date")
            st.stop()
           
        df = session.sql(f"""SELECT 
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
                AND Q.START_TIME BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1,2,3
        ) A 
        GROUP BY 1,2
        ORDER BY 1 ASC
        """).to_pandas()
        # df = df.collect()
        df = df.fillna(0)
        df = df[df['ACTUAL_CREDITS'] != 0]
        df["UTILIZATION"] = (df["EXPECTED_CREDITS"] /
                            df['ACTUAL_CREDITS']).astype(float)
        df["TOTAL_QUERY_S"] = (df["TOTAL_ELAPSED_TIME_MS"]/1000).astype(float)
        df["TOTAL_QUERY_MIN"] = (df["TOTAL_ELAPSED_TIME_MS"]/1000/60).astype(float)
        df["TOTAL_QUERY_HRS"] = (df["TOTAL_ELAPSED_TIME_MS"]/1000/60/60).astype(float)
        df["AVG_QUERY_TIME_MS"] = (df['TOTAL_ELAPSED_TIME_MS'] / df["NUM_QUERIES"]).astype(float)
        df["AVG_QUERY_TIME_S"] = (df["AVG_QUERY_TIME_MS"] / 1000).astype(float)
        df["AVG_CREDITS_PER_QUERY"] = (df['ACTUAL_CREDITS'] / df["NUM_QUERIES"]).astype(float)

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
        df['WAREHOUSE_CPH']=df['WAREHOUSE_CPH'].astype(int)
        df["TOTAL_WH_HRS"] = (df['ACTUAL_CREDITS'] / df["WAREHOUSE_CPH"]).astype(float)
        df["TOTAL_WH_MIN"] = (df["TOTAL_WH_HRS"] * 60).astype(float)
        df["TOTAL_WH_S"] = (df["TOTAL_WH_MIN"] * 60).astype(float)

        df['NUM_QUERIES']=df['NUM_QUERIES'].astype(int)
        df['EXPECTED_CREDITS']=df['EXPECTED_CREDITS'].astype(float)
        df['ACTUAL_CREDITS']=df['ACTUAL_CREDITS'].astype(float)
        

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

        st.dataframe(df[[
                    'WAREHOUSE_NAME',
                    'WAREHOUSE_SIZE',
                    'NUM_QUERIES',
                    'TOTAL_QUERY_HRS',
                    'TOTAL_WH_HRS',
                    'AVG_CREDITS_PER_QUERY',
                    'AVG_QUERY_TIME_S',
                    'EXPECTED_CREDITS',
                    'ACTUAL_CREDITS',
                    'UTILIZATION',
                ]].sort_values(by=['UTILIZATION'], ascending=False).style.background_gradient(axis=0).format({
                    'NUM_QUERIES': '{:,.0f}',
                    'EXPECTED_CREDITS': '{:,.2f}',
                    'UTILIZATION': '{:,.0%}',
                    'TOTAL_QUERY_HRS': '{:,.2f}',
                    'TOTAL_WH_HRS': '{:,.2f}',
                    'ACTUAL_CREDITS': '{:,.2f}',
                    'AVG_QUERY_TIME_S': '{:,.3f}',
                    'AVG_CREDITS_PER_QUERY': '{:,.2f}',
                }), use_container_width=True)
        st.write('{:,.0f} records'.format(
                    len(df)))
        with st.expander("Column Definitions", False):
                st.markdown(
                """
***WAREHOUSE_NAME***: The name of the warehouse used to execute queries.

***WAREHOUSE_SIZE***: The size of the warehouse used to execute queries. See [Warehouse Size](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#warehouse-size)
                    
***NUM_QUERIES***: Number of queries executed using a warehouse.
* ***Note***: Queries which did not need a warehouse such as those using the [results cache](https://docs.snowflake.com/en/user-guide/querying-persisted-results.html) are not included in this number.

***TOTAL_QUERY_HRS***: From the `query_history` view, the of the `TOTAL_ELAPSED_TIME` column

***TOTAL_WH_HRS***: Derived from the `CREDITS_USED` column in the `warehouse_metering_history` view, taking the `CREDITS_USED` divided by the [Credits Per Hour](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#warehouse-size) to get the uptime of the warehouse.

***AVG_CREDITS_PER_QUERY***: Calculation of `ACTUAL_CREDITS` / `NUM_QUERIES`.

***AVG_QUERY_TIME_S***: Calculation of (`TOTAL_QUERY_HRS` * 60 * 60) / `NUM_QUERIES`.

***EXPECTED_CREDITS***: Calculation of `TOTAL_QUERY_HRS` * (Credits Per Warehouse)

***ACTUAL_CREDITS***: Sum of the `CREDITS_USED` column in the `warehouse_metering_history` view.

***UTILIZATION***: Calculation of `EXPECTED_CREDITS` / `ACTUAL_CREDITS`.
        """)

        st.empty()
        st.empty()
        st.empty()
        
        st.header("Warehouse Metering")
        st.caption("The following visualization pulls data from the WAREHOUSE_METERING_HISTORY view")
        wh_metering = session.table("WAREHOUSE_METERING_HISTORY")
        wh_metering = wh_metering.filter((col("START_TIME")>=start_date) & (col("START_TIME")<=end_date) )
        wh_metering = wh_metering.toPandas()
        wh_metering["START_TIME"] = pd.to_datetime(wh_metering["START_TIME"])
        
        st.vega_lite_chart(wh_metering,
                           {
                               "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                               "vconcat": [
                                   {
                                       "mark": {
                                           "type": "bar", "tooltip": {"content": "data"}
                                           },
                                       "height": 60,
                                       "width": "container",
                                       "autosize": {
                                           "type": "fit",
                                           "contains": "padding"
                                       },
                                       "params": [
                                            {
                                                "name": "brush",
                                                "select": {
                                                    "type": "interval",
                                                    "encodings": ["x"]
                                                }
                                            }
                                        ],
                                       "encoding": {
                                           "x": {
                                               "field": "START_TIME",
                                               "axis": None,
                                               "timeUnit": "utcyearmonthdatehours",
                                               "scale": {"type": "utc"},
                                               "title": "Time"
                                           },
                                           "y": {
                                               "aggregate": "sum",
                                               "title": "",
                                               "field": "CREDITS_USED",
                                           },
                                           "tooltip": [
                                               {
                                                   "field": "START_TIME",
                                                   "title": "Day",
                                                   "scale": {"type": "utc"},
                                                   "formatType": "time",
                                                   "format": "%m %d, %Y" 
                                               },
                                               {
                                                   "aggregate": "sum",
                                                   "field":  "CREDITS_USED",
                                                   "title": "Credits",
                                                   "format": ",.0f"
                                               }
                                           ]
                                       }
                                   },
                                   {

                                       "hconcat": [
                                           {
                                               "mark": {"type": "rect", "tooltip": {"content": "data"}},
                                               "width": "container",
                                               "autosize": {
                                                   "type": "fit",
                                                   "contains": "padding"
                                               },
                                               "height":len(wh_metering["WAREHOUSE_NAME"].unique().tolist())*25,
                                               "encoding": {
                                                   "x": {
                                                       "field": "START_TIME",
                                                       "timeUnit": "utcyearmonthdatehours",
                                                        "scale": {
                                                            "type": "utc",
                                                            "domain": {"param": "brush"},
                                                        },  
                                                       "title": "Time"
                                                   },
                                                   "y":  {
                                                        "field": "WAREHOUSE_NAME",
                                                        "title": "Wahouse Name"
                                                    },
                                                   "color": {
                                                       "field": 'CREDITS_USED',
                                                       "aggregate": "sum"
                                                   },
                                                   "tooltip": [
                                                        {
            "field": "WAREHOUSE_NAME",
            "title": "Wahouse Name"
        },
                                                       {
                                                           "field": "START_TIME",
                                                           "title": "Time",
                                                           "formatType": "time",
                                                           "format": "%m %d, %Y"
                                                       },
                                                       {
                                                           "aggregate": "sum",
                                                           "field":  "CREDITS_USED",
                                                           "title": "Credits",
                                                           "format": ",.0f"
                                                       }
                                                   ]
                                               }
                                           },
                                           {
                                               "mark": {"type": "bar", "tooltip": {"content": "data"}},
                                               "width": 60,
                                            #    "height": height,
                                               "encoding": {
                                                   "y": {
                                                        "field": "WAREHOUSE_NAME",
                                                        "title": "Wahouse Name",
                                                        "axis": None
                                                    },
                                                   "x": {
                                                       "aggregate": "sum",
                                                       "field": "CREDITS_USED",
                                                   },
                                                   "tooltip": [
                                                       {
                                                            "field": "WAREHOUSE_NAME",
                                                            "title": "Wahouse Name"
                                                        },
                                                       {
                                                           "aggregate": "sum",
                                                           "field":  "CREDITS_USED",
                                                           "title": "Credits",
                                                           "format": ",.0f"
                                                       }
                                                   ]
                                               }
                                           }
                                       ]
                                   },
                               ],
                               "config": {
                                   "view": {
                                       "stroke": "transparent"
                                   }
                               }
                           },
                           use_container_width=True
                           )
        
    except Exception as e:
        pass
        # st.write(e)

    st.caption(disclaimer + " The metrics shown on this page should be used as information only. Please work with your Snowflake Account team if you have any questions.")