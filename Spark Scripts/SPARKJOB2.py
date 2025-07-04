# DOCKER SPARKJOB 2: READ BRONZE, CLEAN, WRITE DELTA TO SILVER

# -------CONFIGURE SPARK-------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, trim, regexp_replace, concat_ws, when

spark = SparkSession.builder \
    .appName("BRONZE to SILVER") \
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
afiliacion_usuarios_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/afiliacion_usuarios")
distribuidores_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/distribuidores")
distribuidores_perfiles_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/distribuidores_perfiles")
referidos_origenes_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/referidos_origenes")
prospectos_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/prospectos")
regiones_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/regiones")
zonas_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/zonas")
sucursales_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/sucursales")
ventas_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/ventas")
referidos_asignaciones_df = spark.read.format("delta").load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/referidos_asignaciones")


# -------SELECT, FILTER, WITHCOLUMN, FILLNA----------------------------------------------------------------------------------------------------------------------------------
silver_afiliacion_usuarios_df = afiliacion_usuarios_df \
    .select("id_usuarioAfiliacion", "nombreUsuario")

silver_sucursales_df = sucursales_df \
    .withColumn("estatus", regexp_replace(col("estatus"), r"\r|\n", "")) \
    .filter(col("estatus") == "ACTIVO") \
    .select("id_sucursal", "descripcionSucursal", "id_zona", "id_region")

silver_distribuidores_df = distribuidores_df \
    .filter(regexp_replace(col("estatus"), r"\r|\n", "") == "activo") \
    .select("id_distribuidor", "id_prospecto", "estatus") 

silver_prospectos_df = prospectos_df \
    .filter(regexp_replace(col("estatus"), r"\r|\n", "") == "ACTIVO") \
    .filter(col("updated_at") > "2024-01-01 00:00:00") \
    .select("id_prospecto", "id_referido", "created_at", "id_origen", "id_perfil", "primerNombre","segundoNombre", "primerApellido", "segundoApellido", "id_sucursal") \
    .fillna({"segundoNombre": ""}) \
    .withColumn("NombreCompleto", concat_ws(" ", col("primerNombre"), when(col("segundoNombre") != "", col("segundoNombre")), col("primerApellido"), col("segundoApellido")))

silver_distribuidores_perfiles_df = distribuidores_perfiles_df \
    .filter(col("estatus") == "ACTIVO") \
    .select("id_perfil", "descripcionPerfil")

silver_referidos_origenes_df = referidos_origenes_df \
    .filter(col("estatus") == "ACTIVO") \
    .select("id_origen", "descripcionOrigen")

silver_regiones_df = regiones_df \
    .select("id_region", "descripcionRegion")

silver_zonas_df = zonas_df \
    .filter(regexp_replace(col("estatus"), r"\r|\n", "") == "ACTIVO") \
    .select("id_zona", "descripcionZona", "id_region")

silver_ventas_df = ventas_df \
    .filter(col("fechaVenta") > "2024-01-01 00:00:00") \
    .select("id_venta", "fechaVenta", "id_distribuidor")

silver_referidos_asignaciones_df = referidos_asignaciones_df \
    .filter(col("fechaAsignacion") > "2024-01-01 00:00:00") \
    .select(col("id_asignacion"), col("fechaAsignacion"), col("id_referido"), col("id_usuarioAfiliacion"),)

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

# -------WRITE 1----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_afiliacion_usuarios_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/afiliacion_usuarios",
    "target.id_usuarioAfiliacion = source.id_usuarioAfiliacion"
)
# -------WRITE 2----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_distribuidores_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/distribuidores",
    "target.id_distribuidor = source.id_distribuidor"
)
# -------WRITE 3----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_distribuidores_perfiles_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/distribuidores_perfiles",
    "target.id_perfil = source.id_perfil"
)
# -------WRITE 4----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_prospectos_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/prospectos",
    "target.id_prospecto = source.id_prospecto"
)
# -------WRITE 5----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_referidos_origenes_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/referidos_origenes",
    "target.id_origen = source.id_origen"
)
# -------WRITE 6----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_regiones_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/regiones",
    "target.id_region = source.id_region"
)
# -------WRITE 7----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_zonas_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/zonas",
    "target.id_zona = source.id_zona"
)
# -------WRITE 8----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_sucursales_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/sucursales",
    "target.id_sucursal = source.id_sucursal"
)
# -------WRITE 9----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_ventas_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/ventas",
    "target.id_venta = source.id_venta"
)

# -------WRITE 10----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    silver_referidos_asignaciones_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/SILVER_BUCKET/referidos_asignaciones",
    "target.id_asignacion = source.id_asignacion"
)
spark.stop()