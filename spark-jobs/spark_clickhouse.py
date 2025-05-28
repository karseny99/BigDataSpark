from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from cfg import Config

class DataProcessor:
    def __init__(self):
        self.spark = self._init_spark_session()
        self.config = Config()
        self.source_data = self._load_source_data()
        
    def _init_spark_session(self):
        return SparkSession.builder \
            .appName("DataAnalyticsPipeline") \
            .config("spark.jars", 
                   "/opt/spark/jars/postgresql-42.6.0.jar,"
                   "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar") \
            .getOrCreate()
    
    def _load_source_data(self):
        return {
            "sales": self._read_table(self.config.TABLES["sales"]),
            "products": self._read_table(self.config.TABLES["products"]),
            "customers": self._read_table(self.config.TABLES["customers"]),
            "dates": self._read_table(self.config.TABLES["dates"]),
            "stores": self._read_table(self.config.TABLES["stores"]),
            "suppliers": self._read_table(self.config.TABLES["suppliers"])
        }
    
    def _read_table(self, table_name):
        return self.spark.read.jdbc(
            url=self.config.POSTGRES["url"],
            table=table_name,
            properties={
                "user": self.config.POSTGRES["user"],
                "password": self.config.POSTGRES["password"],
                "driver": self.config.POSTGRES["driver"]
            }
        )
    
    def _write_to_clickhouse(self, df, table_name):
        df.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", self.config.CLICKHOUSE["url"]) \
            .option("user", self.config.CLICKHOUSE["user"]) \
            .option("password", self.config.CLICKHOUSE["password"]) \
            .option("dbtable", table_name) \
            .option("driver", self.config.CLICKHOUSE["driver"]) \
            .option("createTableOptions", self.config.CLICKHOUSE["engine"]) \
            .save()
    
    def process_product_analytics(self):
        # Top selling products
        top_products = self._get_top_selling_products()
        self._write_to_clickhouse(top_products, self.config.TABLES["top_products"])
        
        # Revenue by category
        category_revenue = self._get_revenue_by_category()
        self._write_to_clickhouse(category_revenue, self.config.TABLES["category_revenue"])
        
        # Product ratings
        product_ratings = self._get_product_ratings()
        self._write_to_clickhouse(product_ratings, self.config.TABLES["product_ratings"])
        
        # Highest and lowest rated products
        highest_rated = self._get_highest_rated_products()
        self._write_to_clickhouse(highest_rated, self.config.TABLES["highest_rated"])
        
        lowest_rated = self._get_lowest_rated_products()
        self._write_to_clickhouse(lowest_rated, self.config.TABLES["lowest_rated"])
        
        # Most reviewed products
        most_reviewed = self._get_most_reviewed_products()
        self._write_to_clickhouse(most_reviewed, self.config.TABLES["most_reviewed"])
        
        # Rating-sales correlation
        correlation = self._get_rating_sales_correlation()
        self._write_to_clickhouse(correlation, self.config.TABLES["rating_correlation"])
    
    def process_customer_analytics(self):
        # Top customers by spending
        top_customers = self._get_top_customers()
        self._write_to_clickhouse(top_customers, self.config.TABLES["top_customers"])
        
        # Customer distribution by country
        customer_distribution = self._get_customer_distribution()
        self._write_to_clickhouse(customer_distribution, self.config.TABLES["customer_distribution"])
        
        # Average check per customer
        avg_check = self._get_customer_avg_check()
        self._write_to_clickhouse(avg_check, self.config.TABLES["customer_avg_check"])
    
    def process_time_analytics(self):
        # Monthly trends
        monthly_trends = self._get_monthly_trends()
        self._write_to_clickhouse(monthly_trends, self.config.TABLES["monthly_trends"])
        
        # Yearly trends
        yearly_trends = self._get_yearly_trends()
        self._write_to_clickhouse(yearly_trends, self.config.TABLES["yearly_trends"])
        
        # Year-over-year comparison
        yoy_trends = self._get_yoy_trends(monthly_trends)
        self._write_to_clickhouse(yoy_trends, self.config.TABLES["yoy_trends"])
        
        # Average order size by month
        avg_order_size = self._get_monthly_avg_order_size(monthly_trends)
        self._write_to_clickhouse(avg_order_size, self.config.TABLES["monthly_avg_order"])
    
    def process_store_analytics(self):
        # Top stores by revenue
        top_stores = self._get_top_stores()
        self._write_to_clickhouse(top_stores, self.config.TABLES["top_stores"])
        
        # Sales distribution by city/country
        sales_distribution = self._get_store_sales_distribution()
        self._write_to_clickhouse(sales_distribution, self.config.TABLES["store_sales_distribution"])
        
        # Average check per store
        store_avg_check = self._get_store_avg_check()
        self._write_to_clickhouse(store_avg_check, self.config.TABLES["store_avg_check"])
    
    def process_supplier_analytics(self):
        # Top suppliers by revenue
        top_suppliers = self._get_top_suppliers()
        self._write_to_clickhouse(top_suppliers, self.config.TABLES["top_suppliers"])
        
        # Average price per supplier
        avg_price = self._get_supplier_avg_price()
        self._write_to_clickhouse(avg_price, self.config.TABLES["supplier_avg_price"])
        
        # Sales distribution by supplier country
        supplier_sales = self._get_supplier_sales_distribution()
        self._write_to_clickhouse(supplier_sales, self.config.TABLES["supplier_sales_distribution"])
    
    def _get_top_selling_products(self):
        return self.source_data["sales"].groupBy("product_sk") \
            .agg(
                F.sum("sale_quantity").alias("total_quantity"),
                F.sum("sale_total_price").alias("total_revenue")
            ) \
            .join(self.source_data["products"], "product_sk") \
            .select("product_id", "name", "category", "total_quantity", "total_revenue") \
            .orderBy(F.desc("total_quantity")) \
            .limit(self.config.TOP_N["products"])
    
    def _get_revenue_by_category(self):
        return self.source_data["sales"].join(self.source_data["products"], "product_sk") \
            .groupBy("category") \
            .agg(F.sum("sale_total_price").alias("total_revenue")) \
            .orderBy(F.desc("total_revenue"))
    
    def _get_product_ratings(self):
        return self.source_data["products"].select(
            "product_id", "name", "category", "rating", "reviews"
        )
    
    def _get_highest_rated_products(self):
        return self.source_data["products"].orderBy(F.desc("rating")) \
            .limit(self.config.TOP_N["rated_products"]) \
            .select("product_id", "name", "rating")
    
    def _get_lowest_rated_products(self):
        return self.source_data["products"].orderBy(F.asc("rating")) \
            .limit(self.config.TOP_N["rated_products"]) \
            .select("product_id", "name", "rating")
    
    def _get_most_reviewed_products(self):
        return self.source_data["products"].orderBy(F.desc("reviews")) \
            .limit(self.config.TOP_N["reviewed_products"]) \
            .select("product_id", "name", "reviews")
    
    def _get_rating_sales_correlation(self):
        rating_sales = self.source_data["sales"].join(self.source_data["products"], "product_sk") \
            .groupBy("product_id", "name") \
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.sum("sale_quantity").alias("total_quantity")
            )
        corr_value = rating_sales.stat.corr("avg_rating", "total_quantity")
        return self.spark.createDataFrame([(corr_value,)], ["rating_sales_correlation"])
    
    def _get_top_customers(self):
        return self.source_data["sales"].groupBy("customer_sk") \
            .agg(F.sum("sale_total_price").alias("total_spent")) \
            .join(self.source_data["customers"], "customer_sk") \
            .select("customer_id", "first_name", "last_name", "country", "total_spent") \
            .orderBy(F.desc("total_spent")) \
            .limit(self.config.TOP_N["customers"])
    
    def _get_customer_distribution(self):
        return self.source_data["customers"].groupBy("country") \
            .agg(F.countDistinct("customer_id").alias("num_customers")) \
            .orderBy(F.desc("num_customers"))
    
    def _get_customer_avg_check(self):
        return self.source_data["sales"].groupBy("customer_sk") \
            .agg((F.sum("sale_total_price") / F.count("sale_quantity")).alias("avg_check")) \
            .join(self.source_data["customers"], "customer_sk") \
            .select("customer_id", "avg_check")
    
    def _get_monthly_trends(self):
        return self.source_data["sales"].join(self.source_data["dates"], "date_sk") \
            .groupBy("year", "month") \
            .agg(
                F.sum("sale_total_price").alias("revenue"),
                F.sum("sale_quantity").alias("quantity")
            ) \
            .orderBy("year", "month")
    
    def _get_yearly_trends(self):
        return self.source_data["sales"].join(self.source_data["dates"], "date_sk") \
            .groupBy("year") \
            .agg(
                F.sum("sale_total_price").alias("revenue"),
                F.sum("sale_quantity").alias("quantity")
            ) \
            .orderBy("year")
    
    def _get_yoy_trends(self, monthly_trends):
        return monthly_trends.withColumn(
            "prev_year_revenue",
            F.lag("revenue").over(Window.partitionBy("month").orderBy("year"))
        ).na.fill({"prev_year_revenue": 0.0}) \
         .withColumn(
            "yoy_change",
            (F.col("revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue")
         ).na.fill({"yoy_change": 0.0}) \
         .select("year", "month", "revenue", "prev_year_revenue", "yoy_change")
    
    def _get_monthly_avg_order_size(self, monthly_trends):
        return monthly_trends.withColumn(
            "avg_order_size", 
            F.col("revenue") / F.col("quantity")
        ).select("year", "month", "avg_order_size")
    
    def _get_top_stores(self):
        return self.source_data["sales"].groupBy("store_sk") \
            .agg(F.sum("sale_total_price").alias("revenue")) \
            .join(self.source_data["stores"], "store_sk") \
            .select("name", "city", "country", "revenue") \
            .orderBy(F.desc("revenue")) \
            .limit(self.config.TOP_N["stores"])
    
    def _get_store_sales_distribution(self):
        return self.source_data["sales"].join(self.source_data["stores"], "store_sk") \
            .groupBy("city", "country") \
            .agg(
                F.sum("sale_total_price").alias("revenue"),
                F.sum("sale_quantity").alias("quantity")
            ) \
            .orderBy(F.desc("revenue"))
    
    def _get_store_avg_check(self):
        return self.source_data["sales"].groupBy("store_sk") \
            .agg((F.sum("sale_total_price") / F.count("sale_quantity")).alias("avg_check")) \
            .join(self.source_data["stores"], "store_sk") \
            .select("name", "avg_check")
    
    def _get_top_suppliers(self):
        return self.source_data["sales"].groupBy("supplier_sk") \
            .agg(F.sum("sale_total_price").alias("revenue")) \
            .join(self.source_data["suppliers"], "supplier_sk") \
            .select("name", "city", "country", "revenue") \
            .orderBy(F.desc("revenue")) \
            .limit(self.config.TOP_N["suppliers"])
    
    def _get_supplier_avg_price(self):
        return self.source_data["sales"].groupBy("supplier_sk") \
            .agg(F.avg("unit_price").alias("avg_price")) \
            .join(self.source_data["suppliers"], "supplier_sk") \
            .select("name", "avg_price")
    
    def _get_supplier_sales_distribution(self):
        return self.source_data["sales"].join(self.source_data["suppliers"], "supplier_sk") \
            .groupBy("country") \
            .agg(
                F.sum("sale_total_price").alias("revenue"),
                F.sum("sale_quantity").alias("quantity")
            ) \
            .orderBy(F.desc("revenue"))
    
    def run_all_analytics(self):
        """Execute all analytics processes"""
        self.process_product_analytics()
        self.process_customer_analytics()
        self.process_time_analytics()
        self.process_store_analytics()
        self.process_supplier_analytics()
        self.spark.stop()

if __name__ == "__main__":
    processor = DataProcessor()
    processor.run_all_analytics()
