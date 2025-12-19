{{ config(
    query_tag = 'dbt'
) }}

SELECT
    CUSTOMER_ID as CUSTID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE AS PHN_NUMBER,
    CITY,
    STATE
FROM SNOWFLAKE_DBT_DATA.SF_SCHEMA.CUSTOMER 
