# CDPDelta

This project is to fix the Cloudera Spark 3.2 integration with Delta 2.0.1,
Cloudera spark version has altered the interface of CatalogTable from Apache Spark, 
and added the field accessInfo which is causing the incompatibility issue and throwing 
following error:

java.lang.NoSuchMethodError: org.apache.spark.sql.catalyst.catalog.CatalogTable.<init>(Lorg/apache/spark/sql/catalyst/TableIdentifier;Lorg/apache/spark/sql/catalyst/catalog/CatalogTableType;Lorg/apache/spark/sql/catalyst/catalog/CatalogStorageFormat;Lorg/apache/spark/sql/types/StructType;Lscala/Option;Lscala/collection/Seq;Lscala/Option;Ljava/lang/String;JJLjava/lang/String;Lscala/collection/immutable/Map;Lscala/Option;Lscala/Option;Lscala/Option;Lscala/collection/Seq;ZZLscala/collection/immutable/Map;Lscala/Option;)V


   By overriding 2 classes DeltaCatalog and CreateDeltaTableCommand as CDPDeltaCatalog, CDPCreateDeltaTableCommand 
its is working fine for me.

And the spark session creation step would be following

SparkSession sparkSession = SparkSession.builder()
.appName("CDPDelta")
.master("local[1]")
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.CDPDeltaCatalog")
.enableHiveSupport()
.getOrCreate();

This jar can be also be used with spark shell in the same way.

Please check the test for more info.

