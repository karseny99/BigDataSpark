-- product_sales_report
CREATE TABLE IF NOT EXISTS product_sales_report (
    product_id UInt64,
    product_name String,
    product_category String,
    total_sold UInt64,
    total_revenue Decimal(18,2),
    avg_rating Float32,
    total_reviews UInt64
) ENGINE = MergeTree()
ORDER BY (product_id);

-- customer_sales_report
CREATE TABLE IF NOT EXISTS customer_sales_report (
    customer_id UInt64,
    customer_first_name String,
    customer_last_name String,
    customer_country String,
    total_spent Decimal(18,2),
    purchase_count UInt64,
    avg_check Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (customer_id);

-- time_sales_report
CREATE TABLE IF NOT EXISTS time_sales_report (
    year UInt16,
    month UInt8,
    monthly_revenue Decimal(18,2),
    total_sold UInt64,
    avg_order_size Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (year, month);

-- store_sales_report
CREATE TABLE IF NOT EXISTS store_sales_report (
    store_id UInt64,
    store_name String,
    store_city String,
    store_country String,
    total_revenue Decimal(18,2),
    sales_count UInt64,
    avg_check Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (store_id);

-- supplier_sales_report
CREATE TABLE IF NOT EXISTS supplier_sales_report (
    supplier_id UInt64,
    supplier_name String,
    supplier_country String,
    total_revenue Decimal(18,2),
    avg_product_price Decimal(18,2),
    unique_products UInt64
) ENGINE = MergeTree()
ORDER BY (supplier_id);

-- product_quality_report
CREATE TABLE IF NOT EXISTS product_quality_report (
    product_id UInt64,
    product_name String,
    product_category String,
    rating Float32,
    reviews UInt64,
    sales_count UInt64,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (product_id);