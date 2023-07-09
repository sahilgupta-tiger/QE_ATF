readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/target/ar_properties.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT ad_type, id, end_date, created_on, lat, l2, lon, l1, l5, l3, l4, l6, bedrooms, rooms, surface_total, bathrooms, surface_covered, price_period, price, currency, property_type, title, description, operation_type FROM dataview tgt ")
