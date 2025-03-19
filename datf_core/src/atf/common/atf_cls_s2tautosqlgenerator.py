from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,DateType
from atf.common.atf_dc_read_datasources import read_data
from testconfig import *


class S2TAutoLoadScripts:
  
  def __init__(self, s2tobj, tcdict,spark):
    self.s2tobj = s2tobj
    self.tcdict = tcdict 
    self.spark = spark

  def printSummary(self):
    print(f"Mapping Name:{self.s2tobj.mappingName}")
    print(f"Source File Path:{self.s2tobj.sourceFile}")
    print(f"Source File Format:{self.s2tobj.sourceFileFormat}")
    print(f"Source Table Name: {self.s2tobj.sourceTableName}")
    print(f"Stage Table Name: {self.s2tobj.stageTableName}")
    print(f"Target Table Name: {self.s2tobj.targetTableName}") 
    print(f"Exclude Columns: {self.tcdict['excludecolumns']}")
  
  def getSchemaDefinitionSource(self, schemadf):
    schemaStruct = StructType()
    #print("I am at the point")
    #schemadf.printSchema()
    schemadfCollect = schemadf.collect()
    #print(schemadfCollect)
    for row in schemadfCollect:
      #print(row['sourcecolumnname'] + "," +row['sourcecolumndatatype'])
      if row['sourcecolumndatatype'] == "string" :
        schemaStruct.add(StructField(row['sourcecolumnname'],StringType(),True))
      elif row['sourcecolumndatatype'] == "datetime" :
        schemaStruct.add(StructField(row['sourcecolumnname'],DateType(),True))
      elif row['sourcecolumndatatype'] == "double" :
        schemaStruct.add(StructField(row['sourcecolumnname'],DoubleType(),True))
      elif row['sourcecolumndatatype'] == "integer" :
        schemaStruct.add(StructField(row['sourcecolumnname'],IntegerType(),True))
    #print(schemaStruct)
    return schemaStruct
  
  def getSchemaDefinitionStage(self, schemadf):
    schemaStruct = StructType()
    #print("I am at the point")
    #schemadf.printSchema()
    schemadfCollect = schemadf.collect()
    #print(schemadfCollect)
    for row in schemadfCollect:
      #print(row['stagecolumnname'] + "," +row['stagecolumndatatype'])
      if row['stagecolumndatatype'] == "string" :
        schemaStruct.add(StructField(row['stagecolumnname'],StringType(),True))
      elif row['stagecolumndatatype'] == "datetime" :
        schemaStruct.add(StructField(row['stagecolumnname'],DateType(),True))
      elif row['stagecolumndatatype'] == "double" :
        schemaStruct.add(StructField(row['stagecolumnname'],DoubleType(),True))
      elif row['stagecolumndatatype'] == "integer" :
        schemaStruct.add(StructField(row['stagecolumnname'],IntegerType(),True))
    #print(schemaStruct)
    return schemaStruct

  def getSchemaDefinitionTarget(self, schemadf):
    schemaStruct = StructType()
    #print("I am at the point")
    #schemadf.printSchema()
    schemadfCollect = schemadf.collect()
    #print(schemadfCollect)
    for row in schemadfCollect:
      #print(row['stagecolumnname'] + "," +row['targetcolumndatatype'])
      if row['targetcolumndatatype'] == "string" :
        schemaStruct.add(StructField(row['targetcolumnname'],StringType(),True))
      elif row['targetcolumndatatype'] == "datetime" :
        schemaStruct.add(StructField(row['targetcolumnname'],DateType(),True))
      elif row['targetcolumndatatype'] == "double" :
        schemaStruct.add(StructField(row['targetcolumnname'],DoubleType(),True))
      elif row['targetcolumndatatype'] == "integer" :
        schemaStruct.add(StructField(row['targetcolumnname'],IntegerType(),True))
    #print(schemaStruct)
    return schemaStruct
  
  
  def getSelectTableCmd(self, loadLayer):
    srccollist=[]
    tgtcollist=[]
    andseparator = ' and '    
    commaseparator=', ' 
    filterlist=[]
    lookuplist=[]    
    excludecollist=[]
    joincols = ""
    
    tcdatafilter = self.tcdict["filter"]
    autoscripttype = self.tcdict["autoscripttype"]
    excludecollist = self.tcdict["excludecolumns"].split(",")
    srcMappingText = self.s2tobj.sourceTableName
    tgtMappingText = self.s2tobj.targetTableName 
    exclusionEnabled = False

    if len(excludecollist) > 1:
      exclusionEnabled = True
    #convertCase = self.s2tobj.convertCase
    if loadLayer == "source_to_stage":
      tgtmapping_df = self.s2tobj.stageschemamap_df
      tgtcolumndatatype = "stagecolumndatatype"
      srccolumndatatype = "sourcecolumndatatype"
      if autoscripttype == "source":
        dataFormat = self.s2tobj.sourceFileFormat
        dataFile = root_path + self.s2tobj.sourceFile
        joincols = self.s2tobj.schema_pddf[(self.s2tobj.schema_pddf['primarykey']=='Y') & (self.s2tobj.schema_pddf['tabletype']=="source")]["columnname"].tolist()
        connectionname = self.s2tobj.sourceConnectionName 
        connectiontype = self.s2tobj.sourceConnectionType
        delimiter=self.s2tobj.sourceFileDelimiter
      elif autoscripttype == "target":
        dataFormat = self.s2tobj.stageFileFormat
        dataFile = root_path + self.s2tobj.stageFile
        joincols = self.s2tobj.schema_pddf[(self.s2tobj.schema_pddf['primarykey']=='Y') & (self.s2tobj.schema_pddf['tabletype']=="stage")]["columnname"].tolist()
        connectionname = self.s2tobj.stageConnectionName 
        connectiontype = self.s2tobj.stageConnectionType
        delimiter=self.s2tobj.stageFileDelimiter
        
    elif loadLayer == "stage_to_target":
      tgtmapping_df = self.s2tobj.targetschemamap_df
      tgtcolumndatatype = "targetcolumndatatype"
      srccolumndatatype = "stagecolumndatatype"      
      if autoscripttype == "source":
        dataFormat = self.s2tobj.stageFileFormat
        dataFile = root_path + self.s2tobj.stageFile
        joincols = self.s2tobj.schema_pddf[(self.s2tobj.schema_pddf['primarykey']=='Y') & (self.s2tobj.schema_pddf['tabletype']=="stage")]["columnname"].tolist()
        connectionname = self.s2tobj.stageConnectionName 
        connectiontype = self.s2tobj.stageConnectionType
        delimiter=self.s2tobj.sourceFileDelimiter
      elif autoscripttype == "target":
        dataFormat = self.s2tobj.targetFileFormat
        dataFile = root_path + self.s2tobj.targetFile
        joincols = self.s2tobj.schema_pddf[(self.s2tobj.schema_pddf['primarykey']=='Y') & (self.s2tobj.schema_pddf['tabletype']=="target")]["columnname"].tolist()
        connectionname =self.s2tobj.targetConnectionName  
        connectiontype = self.s2tobj.targetConnectionType
        delimiter=self.s2tobj.targetFileDelimiter

        
    elif loadLayer == "source_to_target":
      srcTableName = self.s2tobj.sourceTableName
      tgtTableName = self.s2tobj.targetTableName
      tgtmapping_df = self.s2tobj.targetschemamap_df
      tgtcolumndatatype = "targetcolumndatatype"
      srccolumndatatype = "sourcecolumndatatype"
      
      if autoscripttype == "source":
        dataFormat = self.s2tobj.sourceFileFormat
        dataFile = root_path + self.s2tobj.sourceFile
        joincols = self.s2tobj.schema_pddf[(self.s2tobj.schema_pddf['primarykey']=='Y') & (self.s2tobj.schema_pddf['tabletype']=="source")]["columnname"].tolist()
        connectionname = self.s2tobj.sourceConnectionName  
        connectiontype = self.s2tobj.sourceConnectionType
        delimiter=self.s2tobj.sourceFileDelimiter
      elif autoscripttype == "target":
        dataFormat = self.s2tobj.targetFileFormat
        dataFile = root_path + self.s2tobj.targetFile
        joincols = self.s2tobj.schema_pddf[(self.s2tobj.schema_pddf['primarykey']=='Y') & (self.s2tobj.schema_pddf['tabletype']=="target")]["columnname"].tolist()
        connectionname = self.s2tobj.targetConnectionName  
        connectiontype = self.s2tobj.targetConnectionType
        delimiter=self.s2tobj.targetFileDelimiter


    for mapping in tgtmapping_df.rdd.collect():
      srccoltext=""
      tgtcoltext=""
      if exclusionEnabled == True and excludecollist.count(mapping["tgtcolumnname"]) > 0:
        continue
      if mapping["transformationtype"] == "directmapped":
        srccoltext = f'src.{mapping["srccolumnname"]}'
        tgtcoltext = mapping["tgtcolumnname"]
      elif mapping["transformationtype"] == "autogenerated":
        continue        
      elif mapping["transformationtype"] == "derived":
        srccoltext = f'{mapping["selectsqlexpression"]}'
        tgtcoltext = mapping["tgtcolumnname"]      
        if mapping["lookuptablename"] != "":
          lookuptext = f' LEFT JOIN {mapping["lookuptablename"]} ON src.{mapping["srccolumnname"]}={mapping["lookuptablename"]}.{mapping["lookupjoincolumnname"]}'
          lookuplist.append(lookuptext)
          srccoltext = f'{mapping["lookupreturnexpression"]}'
      
      if loadLayer == "stage_to_target" or loadLayer == "source_to_target":
        if (self.s2tobj.trimWhiteSpaces == True and mapping[tgtcolumndatatype].upper() == 'STRING'):
          srccoltext = f'trim({srccoltext})'
        if (mapping[tgtcolumndatatype] != mapping[srccolumndatatype] and mapping[srccolumndatatype] != None):
            srccoltext = f'cast({srccoltext} as {mapping[tgtcolumndatatype]})'
        '''
        if (mapping["convertcase"] != "" and mapping["convertcase"] != self.s2tobj.convertCase):
          convertCase = mapping["convertcase"].upper()      
        if (convertCase == "U" and mapping[tgtcolumndatatype].upper() == 'STRING'):
          srccoltext= f'upper({srccoltext})'
        elif (convertCase == "L" and mapping[tgtcolumndatatype].upper() == 'STRING'):
          srccoltext= f'lower({srccoltext})' 
        '''       
        if mapping["filteroutnulls"].upper() == "Y":
          filterlist.append(f'src.{mapping["srccolumnname"]} IS NOT NULL')      
      
      srccoltext=srccoltext+f' as {mapping["tgtcolumnname"]}'
      srccollist.append(srccoltext)
      tgtcollist.append(tgtcoltext)

    filterClause=andseparator.join(filterlist)
    if filterClause != "" and autoscripttype == "source":
      filterClause=' WHERE '+filterClause
    if tcdatafilter != "":
      if filterClause != "":
        filterClause=filterClause+' AND '+tcdatafilter
      else:
        filterClause=filterClause+' WHERE '+tcdatafilter
    
    lookuplist=list(set(lookuplist))
    lookupClause="".join(lookuplist)
         
    if autoscripttype == "source":
      selcolClause=commaseparator.join(srccollist)
      srcTableName = self.tcdict["path"]+"."+self.tcdict["name"]
      if self.tcdict["format"] == "table":
        self.selectTableCommand=f"SELECT {selcolClause} FROM {srcTableName} src {lookupClause} {filterClause}"
      else:
        self.selectTableCommand=f"SELECT {selcolClause} FROM dataview src {lookupClause} {filterClause}"
      if loadLayer == "source_to_stage":
        schemaStruct= self.getSchemaDefinitionSource(self.s2tobj.srcschema_df)
      else:
        schemaStruct= self.getSchemaDefinitionStage(self.s2tobj.stgschema_df)
        
    elif autoscripttype == "target":
      selcolClause=commaseparator.join(tgtcollist)
      tgtTableName = self.tcdict["path"]+"."+self.tcdict["name"]
      if self.tcdict["format"] == "table":
        self.selectTableCommand=f"SELECT {selcolClause} FROM {tgtTableName} tgt {filterClause}"
      else:
        self.selectTableCommand=f"SELECT {selcolClause} FROM dataview tgt {filterClause}"  
      if loadLayer == "stage_to_target" or loadLayer == "source_to_target":
        schemaStruct= self.getSchemaDefinitionTarget(self.s2tobj.stgschema_df)
      else:
        schemaStruct= self.getSchemaDefinitionStage(self.s2tobj.stgschema_df)
    
    print(self.selectTableCommand)   
    autoscriptpath = self.tcdict['autoscriptpath']
    autoScriptFile = f"{root_path}/test/sql/" + autoscriptpath + '/' + self.tcdict["test_case_name"] + "_" + loadLayer + "_" + self.tcdict["autoscripttype"] +".sql"
    autoScriptFile= autoScriptFile.replace('//','/')

    print(f"Auto SQL Gen: dataformat = {dataFormat}")
    print(f"Auto SQL Gen: datafile = {dataFile}")

    f=open(autoScriptFile,"w+")
    if dataFormat in ["avro","delta","parquet","json","delimitedfile"]:
      if dataFormat == "avro":
        f.write(f"readschemadf=spark.read.format('{dataFormat}').load('{dataFile}').schema\r\n")
        f.write(f"readdatadf=spark.read.format('{dataFormat}').schema(readschemadf).load('{dataFile}')\r\n")
        readschemadf=self.spark.read.format(dataFormat).load(dataFile).schema
        readdatadf=self.spark.read.format(dataFormat).schema(readschemadf).load(dataFile)
      if dataFormat == "parquet":
        print(dataFile)
        f.write(f"readdatadf=spark.read.format('{dataFormat}').load('{dataFile}')\r\n")
        readdatadf=self.spark.read.format(dataFormat).load(dataFile)
      if dataFormat == "delta":
        deltaFile = dataFile.replace(root_path,"")
        if '/' in deltaFile:
          f.write(f"readdatadf=spark.read.format('{dataFormat}').load('{deltaFile}')\r\n")
          readdatadf= self.spark.read.format(dataFormat).load(deltaFile)
        else:
          f.write(f"readdatadf=spark.table('{deltaFile}')\r\n")
          readdatadf= self.spark.table(deltaFile)
      if dataFormat == "json":
        print(dataFile)
        f.write(f"readdatadf=spark.read.format('{dataFormat}').option('multiline','true').load('{dataFile}')\r\n")
        readdatadf=self.spark.read.format(dataFormat).schema(schemaStruct).load(dataFile)
      if dataFormat == "delimitedfile":
        print(dataFile)
        f.write(f"readdatadf=spark.read.format('{dataFormat}').option('delimiter',{delimiter}).option('header','true').load('{dataFile}')\r\n")
        readdatadf=self.spark.read.format("csv").option('delimiter',delimiter).option('header','true').schema(schemaStruct).load(dataFile)
        #readdatadf.printSchema()
      #f.write(f"readdatadf=preproc_unnestfields(readdatadf)\r\n")
      #readdatadf=preproc_unnestfields(readdatadf)
      readdatadf.printSchema()
      f.write(f"readdatadf.createOrReplaceTempView('dataview')\r\n")
      readdatadf.createOrReplaceTempView('dataview')
      f.write(f'spark.sql("{self.selectTableCommand}")\r\n')
      returndf = self.spark.sql(self.selectTableCommand)
    else:
      f.write(f'spark.sql("{self.selectTableCommand}")\r\n')
      returndf, table_query = read_data(self.tcdict,self.spark)
            
    f.close()
    filePath = str(dataFormat) + ".`" +str(dataFile) + "`"
    file_details_dict = {"join_columns":joincols,"file_path":filePath,"connectionname":connectionname, "connectiontype":connectiontype}
    return autoScriptFile, returndf, file_details_dict
  