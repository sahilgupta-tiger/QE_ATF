readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/target/ar_properties.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT ad_type, start_date, id, end_date, lat, created_on, lon, l2, l1, l4, l3, l5, l6, bedrooms, rooms, bathrooms, surface_total, surface_covered, price, currency, price_period, operation_type, title, description, property_type FROM dataview tgt ")
