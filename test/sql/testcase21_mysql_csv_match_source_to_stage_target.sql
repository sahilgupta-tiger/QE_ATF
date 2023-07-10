readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/target/patients_target.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT SSN, id, PREFIX, PASSPORT, DRIVERS, FIRST, SUFFIX, LAST, MAIDEN, MARITAL, RACE, BIRTHPLACE, ETHNICITY, GENDER, ADDRESS, CITY, STATE, COUNTY, ZIP, HEALTHCARE_EXPENSES, LON, HEALTHCARE_COVERAGE, LAT FROM dataview tgt ")
