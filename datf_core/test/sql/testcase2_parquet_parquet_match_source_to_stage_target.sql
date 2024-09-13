readdatadf=spark.read.format('parquet').load('file:/Workspace/Repos/rajat.yadav@tigeranalytics.com/QE_ATF/datf_core/test/data/stage/patients_target_parquet_match')
readdatadf.createOrReplaceTempView('dataview')
