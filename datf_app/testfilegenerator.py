from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, expr, when, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StringType, DateType, FloatType
import random
import string
from pyspark.sql import Row

# ✅ Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("GeneratePatientParquet").getOrCreate()

# ✅ Step 2: Define Function to Generate Random Data
def random_string(length=10):
    """Generate a random string of given length"""
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_ssn():
    """Generate a random SSN (XXX-XX-XXXX)"""
    return f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"

def random_zip():
    """Generate a random ZIP code"""
    return f"{random.randint(10000, 99999)}"

# ✅ Step 3: Generate Sample DataFrame
num_rows = 1_000_000  # Adjust this to generate larger datasets

df = spark.range(0, num_rows).toDF("id") \
    .withColumn("BIRTHDATE", expr("date_sub(current_date(), int(rand() * 36500))")) \
    .withColumn("DEATHDATE", when(rand() < 0.1, expr("date_add(BIRTHDATE, int(rand() * 36500))")).otherwise(lit(None))) \
    .withColumn("SSN", lit(random_ssn())) \
    .withColumn("DRIVERS", when(rand() > 0.2, lit(random_string(10))).otherwise(lit(None))) \
    .withColumn("PASSPORT", when(rand() > 0.3, lit(random_string(9))).otherwise(lit(None))) \
    .withColumn("PREFIX", lit(random.choice(["Mr.", "Ms.", "Dr.", "Mrs.", ""])) ) \
    .withColumn("FIRST", lit(random_string(6))) \
    .withColumn("LAST", lit(random_string(8))) \
    .withColumn("SUFFIX", when(rand() > 0.8, lit(random.choice(["Jr.", "Sr.", "III", ""]))).otherwise(lit(None))) \
    .withColumn("MAIDEN", when(rand() < 0.2, lit(random_string(8))).otherwise(lit(None))) \
    .withColumn("MARITAL", lit(random.choice(["Single", "Married", "Divorced", "Widowed"]))) \
    .withColumn("RACE", lit(random.choice(["White", "Black", "Asian", "Hispanic", "Other",None]))) \
    .withColumn("ETHNICITY", lit(random.choice(["Hispanic", "Non-Hispanic", "Unknown",None]))) \
    .withColumn("GENDER", lit(random.choice(["M", "F"]))) \
    .withColumn("BIRTHPLACE", lit(random_string(10))) \
    .withColumn("ADDRESS", lit(random_string(15))) \
    .withColumn("CITY", lit(random_string(10))) \
    .withColumn("STATE", lit(random.choice(["NY", "CA", "TX", "FL", "IL",None]))) \
    .withColumn("COUNTY", lit(random_string(10))) \
    .withColumn("ZIP", lit(random_zip())) \
    .withColumn("LAT", (rand() * 180 - 90).cast(FloatType())) \
    .withColumn("LON", (rand() * 360 - 180).cast(FloatType())) \
    .withColumn("HEALTHCARE_EXPENSES", (rand() * 100000).cast(FloatType())) \
    .withColumn("HEALTHCARE_COVERAGE", (rand() * 10000).cast(FloatType()))


# ✅ Step 4: Introduce Duplicate Records
df_duplicate = df.limit(1000)  # Select 1000 random rows to duplicate
df = df.union(df_duplicate)  # Add duplicate rows to the dataset

# ✅ Step 5: Save as Parquet File
df.write.mode("overwrite").parquet("datf_core/test/data/target/patient_data_target_parquet")

# ✅ Print Schema and Sample Data
df.printSchema()
df.show(5, truncate=False)

print(f"Generated dataset with {df.count()} rows and {len(df.columns)} columns.")
