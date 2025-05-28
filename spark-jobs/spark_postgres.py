# etl_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from cfg import Config

class ETLProcessor:
    def __init__(self):
        self.config = Config()
        self.spark = self._initialize_spark_session()
        
    def _initialize_spark_session(self):
        return SparkSession.builder \
            .appName("PostgreSQLETL") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
            .getOrCreate()
    
    def _load_source_data(self):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config.POSTGRES["url"]) \
            .option("dbtable", "mock_data") \
            .options(**self.config.POSTGRES["properties"]) \
            .load()
    
    def _create_dimension_table(self, df, dim_config):
        window = Window.partitionBy(dim_config["partition_col"]) \
                      .orderBy(dim_config["order_col"])
        
        dim_df = (
            df
            .select(dim_config["partition_col"], 
                   dim_config["order_col"], 
                   *dim_config["selects"])
            .withColumn("rn", row_number().over(window))
            .filter(col("rn") == 1)
            .drop("rn", dim_config["order_col"])
        )
        
        for old_name, new_name in dim_config["renames"].items():
            dim_df = dim_df.withColumnRenamed(old_name, new_name)
            
        dim_df.write \
            .mode("append") \
            .jdbc(
                url=self.config.POSTGRES["url"],
                table=dim_config["target_table"],
                properties=self.config.POSTGRES["properties"]
            )
        
        return dim_config["target_table"]
    
    def _create_date_dimension(self, df):
        date_dim = (
            df
            .select("sale_date")
            .distinct()
            .withColumn("year", year("sale_date"))
            .withColumn("quarter", quarter("sale_date"))
            .withColumn("month", month("sale_date"))
            .withColumn("day", dayofmonth("sale_date"))
            .withColumn("weekday", dayofweek("sale_date"))
        )
        
        date_dim.write \
            .mode("append") \
            .jdbc(
                url=self.config.POSTGRES["url"],
                table="dim_date",
                properties=self.config.POSTGRES["properties"]
            )
        
        return "dim_date"
    
    def _load_dimension_table(self, table_name):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config.POSTGRES["url"]) \
            .option("dbtable", table_name) \
            .options(**self.config.POSTGRES["properties"]) \
            .load()
    
    def _create_fact_table(self, df):
        dim_tables = {
            "date": self._load_dimension_table("dim_date"),
            "customer": self._load_dimension_table("dim_customer"),
            "seller": self._load_dimension_table("dim_seller"),
            "product": self._load_dimension_table("dim_product"),
            "store": self._load_dimension_table("dim_store"),
            "supplier": self._load_dimension_table("dim_supplier")
        }
        
        fact_df = (
            df
            .join(dim_tables["date"],    df.sale_date == dim_tables["date"].sale_date)
            .join(dim_tables["customer"], df.sale_customer_id == dim_tables["customer"].customer_id)
            .join(dim_tables["seller"],   df.sale_seller_id == dim_tables["seller"].seller_id)
            .join(dim_tables["product"],  df.sale_product_id == dim_tables["product"].product_id)
            .join(dim_tables["store"],    df.store_name == dim_tables["store"].name)
            .join(dim_tables["supplier"], df.supplier_name == dim_tables["supplier"].name)
            .select(
                dim_tables["date"].date_sk.alias("date_sk"),
                dim_tables["customer"].customer_sk.alias("customer_sk"),
                dim_tables["seller"].seller_sk.alias("seller_sk"),
                dim_tables["product"].product_sk.alias("product_sk"),
                dim_tables["store"].store_sk.alias("store_sk"),
                dim_tables["supplier"].supplier_sk.alias("supplier_sk"),
                col("sale_quantity"),
                col("sale_total_price"),
                col("unit_price")
            )
        )
        
        fact_df.write \
            .mode("append") \
            .jdbc(
                url=self.config.POSTGRES["url"],
                table="fact_sales",
                properties=self.config.POSTGRES["properties"]
            )
    
    def execute_etl(self):
        try:
            source_df = self._load_source_data()
            
            for dim_name, dim_config in self.config.DIMENSIONS.items():
                print(f"Creating dimension: {dim_name}")
                self._create_dimension_table(source_df, dim_config)
            
            print("Creating date dimension")
            self._create_date_dimension(source_df)
            
            print("Creating fact table")
            self._create_fact_table(source_df)
            
            print("ETL process completed successfully")
            
        except Exception as e:
            print(f"Error during ETL process: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    etl_processor = ETLProcessor()
    etl_processor.execute_etl()
