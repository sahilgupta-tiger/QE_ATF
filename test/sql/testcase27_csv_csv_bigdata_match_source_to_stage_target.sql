readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/target/ar_properties.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT ad_type, bathrooms, bedrooms, created_on, currency, end_date, id, l1, l2, l3, l4, l5, l6, lat, lon, price, price_period, property_type, rooms, surface_covered, surface_total, title FROM dataview tgt ")
