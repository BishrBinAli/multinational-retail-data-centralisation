# %%
from database_utils import DatabaseConnector
import pandas as pd

# %%
if __name__ == '__main__':
    # %%
    DBConnector_local = DatabaseConnector('db_creds_local.yaml')
    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT country_code, COUNT(*) AS total_no_stores
        FROM dim_store_details
        GROUP BY country_code
        ORDER BY total_no_stores DESC
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT locality, COUNT(*) AS total_no_stores
        FROM dim_store_details
        GROUP BY locality
        HAVING COUNT(*) > 9
        ORDER BY total_no_stores DESC, SUM(staff_numbers) ASC
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT round(SUM(orders.product_quantity * products.product_price)) AS total_sales, dates.month
        FROM orders_table AS orders
        JOIN dim_products AS products 
        ON orders.product_code = products.product_code
        JOIN dim_date_times AS dates
        ON orders.date_uuid = dates.date_uuid
        GROUP BY month
        ORDER BY total_sales DESC
        LIMIT 6
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT COUNT(orders.*) AS number_of_sales, 
            SUM(orders.product_quantity) AS product_quantity_count,
            CASE 
            WHEN stores.store_type = 'Web Portal' THEN 'Web'
            ELSE 'Offline'
            END AS location
        FROM orders_table AS orders
        JOIN dim_store_details AS stores
        ON orders.store_code = stores.store_code
        GROUP BY location
        ORDER BY number_of_sales
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        WITH store_type_sales AS (
            SELECT stores.store_type, round(SUM(orders.product_quantity * products.product_price)::numeric,2) AS total_sales
            FROM orders_table AS orders
            JOIN dim_products AS products 
            ON orders.product_code = products.product_code
            JOIN dim_store_details AS stores
            ON orders.store_code = stores.store_code
            GROUP BY store_type
            ORDER BY total_sales DESC
        ), total_sales_sum AS (
            SELECT sum(total_sales) AS total_sum FROM store_type_sales
        )
        SELECT store_type, total_sales, round((total_sales/(total_sum))*100::numeric,2) AS "percntage_total(%%)"
        FROM store_type_sales, total_sales_sum
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT round(SUM(orders.product_quantity * products.product_price)::numeric) AS total_sales, dates.year, dates.month
        FROM orders_table AS orders
        JOIN dim_products AS products 
        ON orders.product_code = products.product_code
        JOIN dim_date_times AS dates
        ON orders.date_uuid = dates.date_uuid
        GROUP BY year, month
        ORDER BY total_sales DESC
        LIMIT 5
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT 
            SUM(staff_numbers) AS total_staff_numbers,
            CASE
                WHEN country_code = 'N/A' THEN 'Web'
                ELSE country_code
            END AS country_code
        FROM dim_store_details
        GROUP BY country_code
        ORDER BY total_staff_numbers DESC
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        SELECT round(SUM(orders.product_quantity * products.product_price)::numeric) AS total_sales, stores.store_type, stores.country_code
        FROM orders_table AS orders
        JOIN dim_products AS products 
        ON orders.product_code = products.product_code
        JOIN dim_store_details AS stores
        ON orders.store_code = stores.store_code
        WHERE stores.country_code = 'DE'
        GROUP BY store_type, country_code
        ORDER BY total_sales 
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

    # %%
    with DBConnector_local.db_engine.connect() as con:
        sql_statement = """
        WITH time_table AS (
            SELECT 
                dates.year, 
                make_date(CAST(dates.year AS INTEGER), CAST(dates.month AS INTEGER), CAST(dates.day AS INTEGER)) + dates.timestamp AS time
            FROM orders_table AS orders
            JOIN dim_date_times AS dates
            ON orders.date_uuid = dates.date_uuid
            ORDER BY time
        ), difference_table AS (
            SELECT year, time, time - LAG(time) OVER(ORDER BY time) AS difference
            FROM time_table
            ORDER BY time
        )
        SELECT year, json_build_object(
            'hours', EXTRACT(hour FROM AVG(difference)),
            'minutes', EXTRACT(minute FROM AVG(difference)),
            'seconds', floor(EXTRACT(second FROM AVG(difference))),
            'milliseconds', to_char(AVG(difference), 'MS')::numeric
        ) AS actual_time_taken
        FROM difference_table 
        GROUP BY year
        ORDER BY AVG(difference) DESC
        LIMIT 5
        """
        result = con.execute(sql_statement).fetchall()
        result_df = pd.DataFrame(result)
        print(result_df)

        # Can also do it as:
        # WITH time_table AS (
        #     SELECT dates.year, dates.month, dates.day, dates.timestamp,
        #         make_date(CAST(dates.year AS INTEGER), CAST(dates.month AS INTEGER), CAST(dates.day AS INTEGER)) + dates.timestamp AS time
        #     FROM orders_table AS orders
        #     JOIN dim_date_times AS dates
        #     ON orders.date_uuid = dates.date_uuid
        #     ORDER BY time
        # ), difference_table AS (
        #     SELECT year, time, time - LAG(time) OVER(ORDER BY time) AS difference
        #     FROM time_table
        #     ORDER BY time
        # )
        # SELECT
        #     year,
        #     to_char(AVG(difference), '"hours": HH, "minutes": MI, "seconds": SS, "milliseconds": MS') AS actual_time_taken
        # FROM difference_table
        # GROUP BY year
        # ORDER BY actual_time_taken DESC
        # LIMIT 5
# %%
