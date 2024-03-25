readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/stage/patients_target_match.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT ADDRESS, BIRTHDATE, BIRTHPLACE, CITY, COUNTY, DEATHDATE, DRIVERS, ETHNICITY, FIRST, GENDER, HEALTHCARE_COVERAGE, HEALTHCARE_EXPENSES, LAST, LAT, LON, MAIDEN, MARITAL, PASSPORT, PREFIX, RACE, SSN, STATE, SUFFIX, ZIP, id FROM dataview tgt ")
