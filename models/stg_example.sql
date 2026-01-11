
{{ config(
    schema='analytics',
    materialized='table'
) }}


WITH base AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email_address,
        modified_date,
        company_name
    FROM {{ source('raw', 'customer_output') }}  -- raw schema from Fivetran
)

SELECT
    customer_id,
    first_name,
    last_name,
    email_address,
    company_name,
    modified_date,
    ROW_NUMBER() OVER (PARTITION BY company_name ORDER BY modified_date DESC) AS company_rank,
    COUNT(*) OVER (PARTITION BY company_name) AS total_customers_in_company
FROM base

