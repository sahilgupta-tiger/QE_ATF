readdatadf=spark.read.format('parquet').load('/app/test/data/stage/patients_target_parquet_mismatch')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT DEATHDATE, HEALTHCARE_EXPENSES, SUFFIX, PASSPORT, CITY, BIRTHPLACE, MAIDEN, MARITAL, DRIVERS, STATE, COUNTY, ADDRESS, PREFIX, ZIP, LON, SSN, LAST, BIRTHDATE, RACE, ETHNICITY, GENDER, id, HEALTHCARE_COVERAGE, LAT, FIRST FROM dataview tgt ")
