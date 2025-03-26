from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr, lit
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random
from pyspark.sql.functions import lit, collect_list, map_from_arrays
import string
import os
spark = SparkSession.builder.appName("GenerateLargeDataset").getOrCreate()
df = spark.read.parquet("datf_core/test/data/source/patient_data_source_parquet/")
df.show(10)
final_df = None
for col_name in df.columns:
    duplicate_values = df.groupBy(col_name).count().filter("count > 1") \
        .withColumn("Column Name", lit(col_name)) \
        .groupBy("Column Name") \
        .agg(collect_list(col(col_name).cast("string")).alias("List of Duplicates"))

    final_df = duplicate_values if final_df is None else final_df.union(duplicate_values)

final_df.show(20)


'''file_path = "datf_core/test/data/source/flight_source_parquet/"
size_in_bytes = os.path.getsize(file_path)
size_in_mb = size_in_bytes / (1024)  # Convert to MB

print(f"File size: {size_in_mb:.2f} MB") '''
