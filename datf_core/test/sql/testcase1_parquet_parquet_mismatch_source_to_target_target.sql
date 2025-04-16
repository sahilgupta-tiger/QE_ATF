readdatadf=spark.read.format('parquet').load('file:/Workspace/Shared/QE_ATF_Enhanced_03/datf_core/test/data/target/patients_target_parquet_mismatch')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT id, BIRTHDATE, DEATHDATE, SSN, DRIVERS, PASSPORT, PREFIX, FIRST, LAST, SUFFIX, MAIDEN, MARITAL, RACE, ETHNICITY, GENDER, BIRTHPLACE, ADDRESS, CITY, STATE, COUNTY, ZIP, LAT, LON, HEALTHCARE_EXPENSES, HEALTHCARE_COVERAGE FROM dataview tgt ")
