readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/target/patients_target.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT id, SSN, DRIVERS, PASSPORT, PREFIX, FIRST, LAST, MAIDEN, MARITAL, SUFFIX, ETHNICITY, GENDER, RACE, BIRTHPLACE, ADDRESS, CITY, ZIP, STATE, COUNTY, HEALTHCARE_EXPENSES, LON, HEALTHCARE_COVERAGE, LAT FROM dataview tgt ")
