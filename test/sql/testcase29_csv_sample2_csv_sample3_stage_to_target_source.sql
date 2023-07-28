readdatadf=spark.read.format('delimitedfile').option('delimiter',,).option('header','true').load('/app/test/data/stage/sample2_additional_Column.csv')
readdatadf.createOrReplaceTempView('dataview')
spark.sql("SELECT  as ColD, src.ColA as ColA, src.ColA as ColA, src.ColB as ColB, src.ColB as ColB, src.ColC as ColC, src.ColC as ColC, src.ColD as ColD FROM dataview src  ")
