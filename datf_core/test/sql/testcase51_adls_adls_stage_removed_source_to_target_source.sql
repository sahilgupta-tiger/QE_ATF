readdatadf=spark.read.format('parquet').load('file:/Volumes/adls-qe/db_atf_qa/adls_atf_volume/source/patients_source_parquet')
readdatadf.createOrReplaceTempView('dataview')
