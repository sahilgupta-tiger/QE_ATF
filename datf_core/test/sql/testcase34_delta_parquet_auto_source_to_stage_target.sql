readdatadf=spark.read.format('delta').load('dbfs:/user/hive/warehouse/healthcare.db/delta_target')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT Id_New, BIRTHDATE, DEATHDATE, SSN, DRIVERS, PASSPORT, PREFIX, FIRST, LAST, SUFFIX, MAIDEN, MARITAL, RACE, ETHNICITY, GENDER, BIRTHPLACE, ADDRESS, CITY, STATE, COUNTY, ZIP, LAT, LON, HEALTHCARE_EXPENSES, HEALTHCARE_COVERAGE FROM dataview tgt ")
