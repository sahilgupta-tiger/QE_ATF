readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/target/ar_properties.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT id, end_date, ad_type, lon, created_on, lat, l1, l2, l5, l4, l3, rooms, bathrooms, l6, bedrooms, currency, price, surface_covered, surface_total, operation_type, price_period, title, description, property_type FROM dataview tgt ")
