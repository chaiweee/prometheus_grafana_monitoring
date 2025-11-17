from prometheus_client import start_http_server, Gauge, Counter, Summary
import snowflake.connector
import time
import os 


# define all the metrics
metric_definitions = {
    "row_count": ('sf_ge_table_rows', 'Row count from FACT_GE_RESULT',['table']),           
    "evaluated_row_count":('sf_ge_evaluated_row_cnt','Evaluated row count', ['table']),
    "failure_rate": ('sf_ge_failure_rate', 'Failure rate', ['table']),
    "pass_rate": ('sf_ge_pass_rate', 'Pass rate', ['table']),
    "coverage":('sf_ge_coverage', 'Coverage', ['table']),
    "failure_per_hour": ('sf_failure_per_hour', 'Failure rate per hour', ['table']),
    "dynamic_table_status": ('sf_dt_status', 'Dynamic Table status', ['table']),
    "failed_row_count": ('sf_ge_failed_row_cnt', 'Total failed row count', ['table'])

}

queries = {
    "row_count": "SELECT  table_name, COUNT(*) FROM FACT_GE_RESULT GROUP BY table_name",
    "evaluated_row_count": "SELECT  table_name, SUM(evaluated_row_count) FROM FACT_GE_RESULT GROUP BY table_name",
    "failure_rate": "SELECT table_name, AVG(failure_percent) FROM FACT_GE_RESULT GROUP BY table_name",
    "pass_rate": "SELECT table_name, 100-AVG(failure_percent) FROM FACT_GE_RESULT GROUP BY table_name",
    "coverage": "WITH tbl_rcnt AS (SELECT table_name, SUM(row_count) AS total_cnt FROM SAP.INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND table_schema = 'L1' GROUP BY table_name), eva_rows AS (SELECT DISTINCT table_name, evaluated_row_count FROM FACT_GE_RESULT WHERE created_at = (SELECT MAX(created_at) FROM FACT_GE_RESULT)), eva_rcnt AS (SELECT table_name, SUM(evaluated_row_count) AS total_eva FROM eva_rows GROUP BY table_name) SELECT aa.table_name, (bb.total_eva / aa.total_cnt) * 100 AS coverage FROM tbl_rcnt aa JOIN eva_rcnt bb ON aa.table_name = bb.table_name",
    "failure_per_hour": "SELECT table_name, SUM(failed_row_count) / NULLIF(DATEDIFF('hour', MIN(created_at), MAX(created_at)), 0) AS failed_rows_per_hour FROM FACT_GE_RESULT GROUP BY table_name ORDER BY failed_rows_per_hour DESC",
    "dynamic_table_status": "SELECT DISTINCT name, CASE WHEN last_completed_refresh_state = 'SUCCEEDED' THEN 1 WHEN last_completed_refresh_state = 'FAILED' THEN 0 ELSE NULL END AS status FROM TABLE(SAP.INFORMATION_SCHEMA.DYNAMIC_TABLES()) WHERE NAME NOT LIKE '%DBT_BACKUP' AND database_name LIKE '%SAP' AND schema_name = 'L1'",
    "failed_row_count": "SELECT table_name, sum(failed_row_count) as fail_rcnt from fact_ge_result group by table_name"
}

# form the defined metrics into the required format
metrics = {
    name: Gauge(info[0], info[1], info[2])
    for name, info in metric_definitions.items()
}

# customized metric to display table in grafana
sf_dt_details = Gauge('sf_dt_details','Snowflake dynamic table details',['table_name', 'msg'])

def get_snowflake_data():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cs = conn.cursor()
    print(metrics)
    try:
        # execute queries for all metrics
        for name, metric in metrics.items():
            print(f"Running query for {name}...")
            cs.execute(queries[name])
            results = cs.fetchall()
            for table_name, value in results:
                metric.labels(table=table_name).set(value if value is not None else 0)
            print(f"Updated metric: {name}")



        #execute query for the customized table metric
        queries_details = """
            select distinct name, 
                CASE WHEN last_completed_refresh_state = 'SUCCEEDED' THEN 1 WHEN last_completed_refresh_state = 'FAILED' THEN 0 ELSE NULL END AS status_code,
                LAST_COMPLETED_REFRESH_STATE_MESSAGE as msg
            from table(sap.information_schema.dynamic_tables())
            WHERE name NOT LIKE '%DBT_BACKUP'
                AND database_name LIKE '%SAP'
                AND schema_name = 'L1';
        """
        cs.execute(queries_details)
        results = cs.fetchall()

        for name, status_code, msg in results:
            numeric_status = float(status_code) if status_code is not None else 0.0

            sf_dt_details.labels(
                table_name=name,
                msg=msg).set(numeric_status)
  
    finally:
        cs.close()
        conn.close()

if __name__ == '__main__':
    start_http_server(8000)
    while True:
        get_snowflake_data()
        time.sleep(300)  # every 5 minutes
