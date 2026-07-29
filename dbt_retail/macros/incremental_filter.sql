{% macro incremental_filter(source_column='ingestion_timestamp', target_column='silver_ingest_time') %}
    {% if is_incremental() %}
        WHERE {{ source_column }} > COALESCE((SELECT MAX({{ target_column }}) FROM {{ this }}), '1970-01-01')
    {% endif %}
{% endmacro %}
