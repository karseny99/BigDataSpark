class Config:
    # Database configurations
    POSTGRES = {
        "url": "jdbc:postgresql://postgres:5432/spark_db",
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
        "properties": {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
    }

    DIMENSIONS = {
        "customer": {
            "partition_col": "sale_customer_id",
            "order_col": "sale_date",
            "selects": [
                "customer_first_name", "customer_last_name",
                "customer_age", "customer_email",
                "customer_country", "customer_postal_code"
            ],
            "renames": {
                "sale_customer_id": "customer_id",
                "customer_first_name": "first_name",
                "customer_last_name": "last_name",
                "customer_age": "age",
                "customer_email": "email",
                "customer_country": "country",
                "customer_postal_code": "postal_code"
            },
            "target_table": "dim_customer"
        },
        "seller": {
            "partition_col": "sale_seller_id",
            "order_col": "sale_date",
            "selects": [
                "seller_first_name", "seller_last_name",
                "seller_email", "seller_country",
                "seller_postal_code"
            ],
            "renames": {
                "sale_seller_id": "seller_id",
                "seller_first_name": "first_name",
                "seller_last_name": "last_name",
                "seller_email": "email",
                "seller_country": "country",
                "seller_postal_code": "postal_code"
            },
            "target_table": "dim_seller"
        },
        "product": {
            "partition_col": "sale_product_id",
            "order_col": "sale_date",
            "selects": [
                "product_name", "product_category", "product_weight",
                "product_color", "product_size", "product_brand",
                "product_material", "product_description",
                "product_rating", "product_reviews",
                "product_release_date", "product_expiry_date",
                "product_price"
            ],
            "renames": {
                "sale_product_id": "product_id",
                "product_name": "name",
                "product_category": "category",
                "product_weight": "weight",
                "product_color": "color",
                "product_size": "size",
                "product_brand": "brand",
                "product_material": "material",
                "product_description": "description",
                "product_rating": "rating",
                "product_reviews": "reviews",
                "product_release_date": "release_date",
                "product_expiry_date": "expiry_date",
                "product_price": "unit_price"
            },
            "target_table": "dim_product"
        },
        "store": {
            "partition_col": "store_name",
            "order_col": "sale_date",
            "selects": [
                "store_location", "store_city", "store_state",
                "store_country", "store_phone", "store_email"
            ],
            "renames": {
                "store_name": "name",
                "store_location": "location",
                "store_city": "city",
                "store_state": "state",
                "store_country": "country",
                "store_phone": "phone",
                "store_email": "email"
            },
            "target_table": "dim_store"
        },
        "supplier": {
            "partition_col": "supplier_name",
            "order_col": "sale_date",
            "selects": [
                "supplier_contact", "supplier_email", "supplier_phone",
                "supplier_address", "supplier_city", "supplier_country"
            ],
            "renames": {
                "supplier_name": "name",
                "supplier_contact": "contact",
                "supplier_email": "email",
                "supplier_phone": "phone",
                "supplier_address": "address",
                "supplier_city": "city",
                "supplier_country": "country"
            },
            "target_table": "dim_supplier"
        }
    }


    
    CLICKHOUSE = {
        "url": "jdbc:clickhouse://clickhouse:8123/default",
        "user": "custom_user",
        "password": "custom_password",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "engine": "ENGINE = Log"
    }
    
    # Table names
    TABLES = {
        "sales": "fact_sales",
        "products": "dim_product",
        "customers": "dim_customer",
        "dates": "dim_date",
        "stores": "dim_store",
        "suppliers": "dim_supplier",
        
        # Output tables
        "top_products": "top10_products",
        "category_revenue": "revenue_by_category",
        "product_ratings": "product_ratings",
        "top_customers": "top10_customers",
        "customer_distribution": "customers_by_country",
        "customer_avg_check": "avg_check_by_customer",
        "monthly_trends": "monthly_trends",
        "yearly_trends": "yearly_trends",
        "yoy_trends": "yoy_trends_by_month",
        "monthly_avg_order": "avg_order_size_by_month",
        "top_stores": "top5_stores",
        "store_sales_distribution": "sales_by_city_country",
        "store_avg_check": "avg_check_by_store",
        "top_suppliers": "top5_suppliers",
        "supplier_avg_price": "avg_price_by_supplier",
        "supplier_sales_distribution": "sales_by_supplier_country",
        "highest_rated": "highest_rated_products",
        "lowest_rated": "lowest_rated_products",
        "rating_correlation": "rating_sales_correlation",
        "most_reviewed": "top_reviewed_products"
    }

    # Query constants
    TOP_N = {
        "products": 10,
        "customers": 10,
        "stores": 5,
        "suppliers": 5,
        "rated_products": 10,
        "reviewed_products": 10
    }
