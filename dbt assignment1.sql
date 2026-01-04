use database dbt_labs;
create or replace schema raw_schema;
--creating a raw_customer table 
CREATE OR REPLACE TABLE raw_customer (
    id INT,
    first_name STRING,
    last_name STRING
);
INSERT INTO raw_customer (id, first_name, last_name) VALUES
    (1, 'Manoj', 'Krishna'),
    (2, 'Riya', 'Sharma'),
    (3, 'Karan', 'Patel'),
    (4, 'Sneha', 'Reddy'),
    (5, 'Amit', 'Verma');

--creating a raw order table
CREATE OR REPLACE TABLE raw_orders (
    id INT,
    user_id INT,
    order_date DATE,
    status STRING
);

INSERT INTO raw_orders (id, user_id, order_date, status) VALUES
    (101, 1, '2024-01-01', 'delivered'),
    (102, 1, '2024-02-15', 'shipped'),
    (103, 2, '2024-03-05', 'delivered'),
    (104, 3, '2024-04-22', 'cancelled'),
    (105, 1, '2024-05-10', 'returned'),
    (106, 4, '2024-06-18', 'delivered'),
    (107, 2, '2024-07-21', 'shipped');

select * from raw_orders;
select * from raw_customer;

select * from source_data;

SHOW SCHEMAS;

SHOW VIEWS IN SCHEMA dbt_labs.dbt_manojkrishna17;

SELECT * FROM dbt_labs.dbt_manojkrishna17.stg_customer;
SELECT * FROM dbt_labs.dbt_manojkrishna17.stg_orders;
SELECT * FROM dbt_labs.dbt_manojkrishna17.CUSTOMER_ORDER_SUMMARY;
;
SELECT
    status,
    COUNT(*) AS total_orders
FROM dbt_labs.dbt_manojkrishna17.CUSTOMER_ORDER_SUMMARY
GROUP BY status;

