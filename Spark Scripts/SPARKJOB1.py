# SPARKJOB 1: READ AVRO STAGING, CAST TYPES, WRITE DELTA TO BRONZE

# -------CONFIGURE SPARK-------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import to_timestamp, to_date
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("STAGING to BRONZE") \
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

# -------READ 1----------------------------------------------------------------------------------------------------------------------------------

afiliacion_usuarios_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/afiliacion_usuarios/_last_write_afiliacion_usuarios")
afiliacion_usuarios_path = afiliacion_usuarios_path_df.first()["write_path"]
afiliacion_usuarios_schema = StructType([
    StructField("id_usuarioAfiliacion", IntegerType(),True),
    StructField("descripcionUsuario", StringType(), True),
    StructField("nombreUsuario", StringType(), True),
    StructField("id_empleado", IntegerType(),True),
    StructField("correo", StringType(), True),
    StructField("estatus", StringType(), True),
])
afiliacion_usuarios_df = spark.read.format("avro") \
    .schema(afiliacion_usuarios_schema) \
    .load(afiliacion_usuarios_path)

# -------READ 2----------------------------------------------------------------------------------------------------------------------------------

distribuidores_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/distribuidores/_last_write_distribuidores")
distribuidores_path = distribuidores_path_df.first()["write_path"]
distribuidores_schema = StructType([
    StructField("id_distribuidor", IntegerType(),True),
    StructField("created_at", StringType(), True),
    StructField("id_perfil", IntegerType(), True),
    StructField("id_prospecto", IntegerType(),True),
    StructField("fechaAlta", StringType(), True),
    StructField("estatus", StringType(), True),
])
distribuidores_df = spark.read.format("avro") \
    .schema(distribuidores_schema) \
    .load(distribuidores_path)
casted_distribuidores_df = distribuidores_df\
    .withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss.S"))\
    .withColumn("fechaAlta", to_date("fechaAlta", "yyyy-MM-dd"))

# -------READ 3----------------------------------------------------------------------------------------------------------------------------------

distribuidores_perfiles_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/distribuidores_perfiles/_last_write_distribuidores_perfiles")
distribuidores_perfiles_path = distribuidores_perfiles_path_df.first()["write_path"]
distribuidores_perfiles_schema = StructType([
    StructField("id_perfil", IntegerType(),True),
    StructField("descripcionPerfil", StringType(), True),
    StructField("estatus", StringType(), True),
    StructField("updated_at", StringType(),True),
])
distribuidores_perfiles_df = spark.read.format("avro") \
    .schema(distribuidores_perfiles_schema) \
    .load(distribuidores_perfiles_path)

# -------READ 4----------------------------------------------------------------------------------------------------------------------------------

prospectos_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/prospectos/_last_write_prospectos")
prospectos_path = prospectos_path_df.first()["write_path"]
prospectos_schema = StructType([
    StructField("id_prospecto", IntegerType(),True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("id_origen", IntegerType(),True),
    StructField("id_referido", IntegerType(), True),
    StructField("primerNombre", StringType(), True),
    StructField("segundoNombre", StringType(),True),
    StructField("primerApellido", StringType(), True),
    StructField("segundoApellido", StringType(), True),
    StructField("curpProspecto", StringType(),True),
    StructField("rfcProspecto", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("correo", StringType(),True),
    StructField("direccion", StringType(), True),
    StructField("salario", StringType(), True),
    StructField("id_perfil", IntegerType(),True),
    StructField("id_sucursal", IntegerType(), True),
    StructField("resultado_historialCredito", StringType(), True),
    StructField("estatus", StringType(), True),
])
prospectos_df = spark.read.format("avro") \
    .schema(prospectos_schema) \
    .load(prospectos_path)

# -------READ 5----------------------------------------------------------------------------------------------------------------------------------

referidos_origenes_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/referidos_origenes/_last_write_referidos_origenes")
referidos_origenes_path = referidos_origenes_path_df.first()["write_path"]
referidos_origenes_schema = StructType([
    StructField("id_origen", IntegerType(),True),
    StructField("descripcionOrigen", StringType(), True),
    StructField("estatus", StringType(), True),
    StructField("updated_at", StringType(),True),
])
referidos_origenes_df = spark.read.format("avro") \
    .schema(referidos_origenes_schema) \
    .load(referidos_origenes_path)

casted_referidos_origenes_df = referidos_origenes_df\
    .withColumn("updated_at", to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss.S"))

# -------READ 6----------------------------------------------------------------------------------------------------------------------------------

regiones_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/regiones/_last_write_regiones")
regiones_path = regiones_path_df.first()["write_path"]
regiones_schema = StructType([
    StructField("id_region", IntegerType(),True),
    StructField("descripcionRegion", StringType(), True),
    StructField("id_empleado", IntegerType(), True),
    StructField("estatus", StringType(),True),
])
regiones_df = spark.read.format("avro") \
    .schema(regiones_schema) \
    .load(regiones_path)

# -------READ 7----------------------------------------------------------------------------------------------------------------------------------

sucursales_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/sucursales/_last_write_sucursales")
sucursales_path = sucursales_path_df.first()["write_path"]
sucursales_schema = StructType([
    StructField("id_sucursal", IntegerType(),True),
    StructField("updated_at", StringType(), True),
    StructField("descripcionSucursal", StringType(), True),
    StructField("id_zona", IntegerType(),True),
    StructField("id_region", IntegerType(),True),
    StructField("ubicacionSucursal", StringType(), True),
    StructField("cpSucursal", StringType(), True),
    StructField("estatus", StringType(), True),
])
sucursales_df = spark.read.format("avro") \
    .schema(sucursales_schema) \
    .load(sucursales_path)

casted_sucursales_df = sucursales_df\
    .withColumn("updated_at", to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss.S"))

# -------READ 8----------------------------------------------------------------------------------------------------------------------------------

zonas_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/zonas/_last_write_zonas")
zonas_path = zonas_path_df.first()["write_path"]
zonas_schema = StructType([
    StructField("id_zona", IntegerType(),True),
    StructField("descripcionZona", StringType(), True),
    StructField("id_region", IntegerType(), True),
    StructField("id_empleado", IntegerType(),True),
    StructField("estatus", StringType(), True),
])
zonas_df = spark.read.format("avro") \
    .schema(zonas_schema) \
    .load(zonas_path)

# -------READ 9----------------------------------------------------------------------------------------------------------------------------------

ventas_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/ventas/_last_write_ventas")
ventas_path = ventas_path_df.first()["write_path"]
ventas_schema = StructType([
    StructField("id_venta", IntegerType(),True),
    StructField("created_at", StringType(), True),
    StructField("fechaVenta", StringType(), True),
    StructField("id_producto", IntegerType(),True),
    StructField("precioProducto", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("id_distribuidor", IntegerType(), True),
    StructField("fechaact_de_ref", StringType(),True),
    StructField("id_cliente", IntegerType(), True),
    StructField("estatus", StringType(), True),
])
ventas_df = spark.read.format("avro") \
    .schema(ventas_schema) \
    .load(ventas_path)

casted_ventas_df = ventas_df\
    .withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss.S"))\
    .withColumn("fechaVenta", to_timestamp("fechaVenta", "yyyy-MM-dd HH:mm:ss.S"))

# -------READ 10----------------------------------------------------------------------------------------------------------------------------------

referidos_asignaciones_path_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/STAGING_BUCKET/referidos_asignaciones/_last_write_referidos_asignaciones")
referidos_asignaciones_path = referidos_asignaciones_path_df.first()["write_path"]
referidos_asignaciones_schema = StructType([
    StructField("id_asignacion", IntegerType(),True),
    StructField("fechaAsignacion", StringType(), True),
    StructField("id_referido", IntegerType(), True),
    StructField("id_usuarioAfiliacion", IntegerType(),True),
    StructField("id_sucursal", IntegerType(),True),
])
referidos_asignaciones_df = spark.read.format("avro") \
    .schema(referidos_asignaciones_schema) \
    .load(referidos_asignaciones_path)

casted_referidos_asignaciones_df = referidos_asignaciones_df\
    .withColumn("fechaAsignacion", to_timestamp("fechaAsignacion", "yyyy-MM-dd HH:mm:ss.S")) 

# -------UPSERT DELTA FUNCTION----------------------------------------------------------------------------------------------------------------------------------

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
    afiliacion_usuarios_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/afiliacion_usuarios",
    "target.id_usuarioAfiliacion = source.id_usuarioAfiliacion"
)
# -------WRITE 2----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    casted_distribuidores_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/distribuidores",
    "target.id_distribuidor = source.id_distribuidor"
)
# -------WRITE 3----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    distribuidores_perfiles_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/distribuidores_perfiles",
    "target.id_perfil = source.id_perfil"
)
# -------WRITE 4----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    prospectos_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/prospectos",
    "target.id_prospecto = source.id_prospecto"
)
# -------WRITE 5----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    casted_referidos_origenes_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/referidos_origenes",
    "target.id_origen = source.id_origen"
)
# -------WRITE 6----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    regiones_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/regiones",
    "target.id_region = source.id_region"
)
# -------WRITE 7----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    zonas_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/zonas",
    "target.id_zona = source.id_zona"
)
# -------WRITE 8----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    casted_sucursales_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/sucursales",
    "target.id_sucursal = source.id_sucursal"
)
# -------WRITE 9----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    casted_ventas_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/ventas",
    "target.id_venta = source.id_venta"
)

# -------WRITE 10----------------------------------------------------------------------------------------------------------------------------------
upsert_to_delta(
    casted_referidos_asignaciones_df,
    "hdfs://hadoop-namenode:9000/user/kerberos/TEST_ETL/BRONZE_BUCKET/referidos_asignaciones",
    "target.id_asignacion = source.id_asignacion"
)

spark.stop()