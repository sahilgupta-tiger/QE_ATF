spark.sql("SELECT src.id as id, src.SSN as SSN, src.DRIVERS as DRIVERS, src.PASSPORT as PASSPORT, src.PREFIX as PREFIX, src.FIRST as FIRST, src.LAST as LAST, src.MAIDEN as MAIDEN, src.MARITAL as MARITAL, src.SUFFIX as SUFFIX, src.ETHNICITY as ETHNICITY, src.GENDER as GENDER, src.RACE as RACE, src.BIRTHPLACE as BIRTHPLACE, src.ADDRESS as ADDRESS, src.CITY as CITY, src.ZIP as ZIP, src.STATE as STATE, src.COUNTY as COUNTY, src.HEALTHCARE_EXPENSES as HEALTHCARE_EXPENSES, src.LON as LON, src.HEALTHCARE_COVERAGE as HEALTHCARE_COVERAGE, src.LAT as LAT FROM patients_db.patients src  ")