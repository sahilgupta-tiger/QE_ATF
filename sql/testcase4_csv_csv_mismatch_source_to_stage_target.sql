readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/stage/patients_target_mismatch.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT DEATHDATE, HEALTHCARE_EXPENSES, SUFFIX, PASSPORT, CITY, BIRTHPLACE, MAIDEN, MARITAL, DRIVERS, STATE, COUNTY, ADDRESS, PREFIX, ZIP, LON, SSN, LAST, BIRTHDATE, RACE, ETHNICITY, GENDER, id, HEALTHCARE_COVERAGE, LAT, FIRST FROM dataview tgt ")
