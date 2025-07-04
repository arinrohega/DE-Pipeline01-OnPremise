# SPARKJOB 3
# READ SILVER, JOIN, AGG, WRITE DELTA TABLES IN GOLD LAYER

# -------CONFIGURE SPARK-------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, min as min_

spark = SparkSession.builder \
    .appName("SILVER to GOLD") \
    .master("local[*]") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "1g") \
    .config("spark.jars", 
             "/opt/delta-jars/delta-spark_2.12-3.2.0.jar,"
             "/opt/delta-jars/delta-storage-3.2.0.jar,"
             "/opt/delta-jars/spark-avro_2.12-3.5.0.jar")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# -------READ----------------------------------------------------------------------------------------------------------------------------------
silver_afiliacion_usuarios_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/afiliacion_usuarios")
silver_distribuidores_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/distribuidores")
silver_distribuidores_perfiles_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/distribuidores_perfiles")
silver_referidos_origenes_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/referidos_origenes")
silver_prospectos_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/prospectos")
silver_regiones_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/regiones")
silver_zonas_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/zonas")
silver_sucursales_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/sucursales")
silver_ventas_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/ventas")
silver_referidos_asignaciones_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/referidos_asignaciones")


# -------JOINSS---------------------------------------------------------------------------------------------------------------------------

gold_df = silver_prospectos_df.join(
    silver_sucursales_df.select("id_sucursal", "descripcionSucursal", "id_zona"),
    on="id_sucursal",
    how="left"
)

gold_df = gold_df.join(
    silver_zonas_df.select("id_zona", "descripcionZona", "id_region"),
    on="id_zona",
    how="left"
)

gold_df = gold_df.join(
    silver_regiones_df.select("id_region", "descripcionRegion"),
    on="id_region",
    how="left"
)

gold_df = gold_df.join(
    silver_referidos_origenes_df.select("id_origen", "descripcionOrigen"),
    on="id_origen",
    how="left"
)

gold_df = gold_df.join(
    silver_distribuidores_perfiles_df.select("id_perfil", "descripcionPerfil"),
    on="id_perfil",
    how="left"
)

gold_df = gold_df.join(
    silver_referidos_asignaciones_df.select("id_referido", "fechaAsignacion"),
    on="id_referido",
    how="left"
)

gold_df = gold_df.join(
    silver_distribuidores_df.select("id_distribuidor", "id_prospecto"),
    on="id_prospecto",
    how="left"
)

agg_ventas_df = silver_ventas_df\
    .groupBy("id_distribuidor")\
    .agg(min_("fechaVenta").alias("FechaActivacion"))

gold_df = gold_df.join(
    agg_ventas_df.select("id_distribuidor", "FechaActivacion"),
    on="id_distribuidor",
    how="left"
)

renamed_gold_df = gold_df.select(
    col("id_prospecto").alias("ID"),
    col("NombreCompleto").alias("NOMBRE_PROSPECTO"),
    col("descripcionOrigen").alias("ORIGEN"),
    col("descripcionPerfil").alias("PERFIL"),
    col("descripcionSucursal").alias("SUCURSAL"),
    col("descripcionZona").alias("ZONA"),
    col("descripcionRegion").alias("REGION"),
    col("fechaAsignacion").alias("FECHA_ASIGNACION"),
    col("created_at").alias("FECHA_CAPTURA"),
    col("FechaActivacion").alias("FECHA_ACTIVACION")  
)

# -------UPSERT DELTA WRITE FUNCTION----------------------------------------------------------------------------------------------------------------------------------

def upsert_to_delta(df, delta_path, merge_condition, partition_columns=None):
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)

# -------WRITE ----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    renamed_gold_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/GOLD_BUCKET/Prospectos_Activaciones",
    "target.ID = source.ID"
)
spark.stop()