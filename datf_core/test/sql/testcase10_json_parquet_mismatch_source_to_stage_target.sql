readdatadf=spark.read.format('parquet').load('/app/test/data/stage/patients_target_parquet_mismatch')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT ADDRESS, BIRTHDATE, BIRTHPLACE, CITY, COUNTY, DEATHDATE, DRIVERS, ETHNICITY, FIRST, GENDER, HEALTHCARE_COVERAGE, HEALTHCARE_EXPENSES, LAST, LAT, LON, MAIDEN, MARITAL, PASSPORT, PREFIX, RACE, SSN, STATE, SUFFIX, ZIP, id FROM dataview tgt ")
