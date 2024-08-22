readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('file:/Workspace/Repos/sahil.gupta@tigeranalytics.com/QE_ATF/datf_core/test/data/target/ar_properties_1.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT id, ad_type, end_date, created_on, lat, lon, l1, l2, l3, l4, l5, l6, rooms, bedrooms, bathrooms, surface_total, surface_covered, price, currency, price_period, title, property_type FROM dataview tgt ")
