readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/stage/sample1.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT src.ColA as ColA, src.ColB as ColB, src.ColC as ColC FROM dataview src  ")
