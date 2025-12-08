WITH run_results AS (
    SELECT
        JSON_VALUE(value, '$.unique_id') AS model_name,
        JSON_VALUE(value, '$.status') AS status,
        CAST(JSON_VALUE(value, '$.execution_time') AS FLOAT) AS execution_time,
        CAST(JSON_VALUE(value, '$.adapter_response.rows_affected') AS BIGINT) AS rows_affected,
        JSON_VALUE(value, '$.relation_name') AS relation_name,
        JSON_VALUE(value, '$.timing[1].started_at') AS started_at,
        JSON_VALUE(value, '$.timing[1].completed_at') AS completed_at,
        JSON_VALUE(metadata, '$.generated_at') AS generated_at,
        JSON_VALUE(metadata, '$.invocation_id') AS invocation_id
    FROM {{ dbt_utils.get_column_values('target_schema.run_results_json', 'value') }}
)

SELECT *
FROM run_results;