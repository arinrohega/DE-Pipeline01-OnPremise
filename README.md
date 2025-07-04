# Data Engineering Pipeline - OnPremise
## Introduction

This project simulates a production-grade data engineering pipeline that automates the extraction of operational data from an OLTP database, to deliver cleaned datasets in a Delta Lake environment. The pipeline is built using modern Big Data applications, containerized to replicate enterprise-level data workflows without OS compatibility issues.

(Designed by Alejandro Rodríguez for educational and skill demonstration purposes)

## Tools Used
- 🐳 Docker Containers 
- 🛢️ MySQL DB
- 🔄 Apache Nifi
- ⚡ Apache Spark
- 🗂️ Apache HDFS with Delta Lake
- ⏱️ Apache Airflow

## Case Scenario
The data consumers (stakeholders and end users) require a dataset to be delivered with the following conditions:

- Frecuency: Daily
- Table Format: Flat Denormalized Table
- Environment: On-Premise
- Source: OLTP database

- Description:

An applicants/prospects dataset that shows the most relevant metrics from their affiliation process, and calculating the date of first purchase which marks their transition to official affiliated status.

## Architecture
![xxxxxxxx drawio](https://github.com/user-attachments/assets/19ef1dfd-c282-4aaf-a2f8-edfa7023d4f1)

## Datasets Used
All tables were made and loaded locally to MySQL database. While all data is fictitious, each element was designed meticulously to replicate a real enterprise model, mirroring the afiliation schema of a company I've previously worked with, mantaining logical relationships and consistency across tables.

![dataset1](https://github.com/user-attachments/assets/309bd941-b8a0-40a0-ac6c-243f393cd3e7)

Theres a glossary at the end, providing English translations of the table names if needed.

## Data Model
### Complete OLTP Database Model
![datamodel1](https://github.com/user-attachments/assets/cc53a22c-a9ae-44d2-9b59-fb38ac1bdeeb)

### Chart for desired Table
![etlclient drawio](https://github.com/user-attachments/assets/9ec63cda-8cea-4b85-8741-d7cc2f0cb402)

## Proyect Showcase Video
www.youtube.com 
## Proyect Showcase Guide 
### 1. Docker Containers 🐳

### 1.1 Volumes for Docker 🐳
Using the Docker Desktop app, the proyect was named "apache-stack" using the path "C:\docker\apache-stack"

The repository files [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) and [Dockerfile](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/Dockerfile) are required to build-up the containers, so they were mounted locally like this:

          "C:\docker\apache-stack\docker-compose.yml"    
          "C:\docker\apache-stack\Dockerfile"  

### 1.2 Building-up Containers 🐳

On CMD, the following command was executed to build the containers:

          C:\docker\apache-stack>docker-compose up -d   

The containers were successfully deployed and visible in Docker Desktop:

![docker1](https://github.com/user-attachments/assets/392775c9-191b-4b5a-8eda-e26786b2bc0d)


### 2. Apache HDFS 🗂️

### 2.1 Volumes for Hadoop User Experience (HUE) 🗂️

Considering that the current [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:

          hue:  
            volumes:  
              - ./shared-data/hue.ini:/usr/share/hue/desktop/conf/hue.ini  
              - ./shared-data/hue-data:/hue  

The following Repository File was mounted locally for the volumes to work:
  
          "C:\docker\apache-stack\shared-data\hue.ini"

### 2.2 Creating Medallion Folders in HDFS using HUE🗂️

After confirming that the hue, hadoop-datanode, and hadoop-namenode containers were running, the HUE web interface was accessed via http://localhost:8888/

A directory was created and named by selecting Files > New > Directory. 

![hue1](https://github.com/user-attachments/assets/d8e58d82-68f5-4494-98e1-0c6afeaa7df2)

![hue99](https://github.com/user-attachments/assets/823dfe71-c902-486a-aef7-3a6c4e092d8c)


To mirror a modern delta lake, the following directories were created inside the main one, naming 3 of them with a medallion hierarchy and 1 for staging or landing

![hue2](https://github.com/user-attachments/assets/11a6d07c-b0d1-4e66-926b-f9a968c2857a)


### 3. Apache NIFI 🔄

### 3.1 Objective🔄

The main goal of each Process Group in NIFI is:  

1) Read a table from MySQL  
2) Write it to the HDFS Staging Bucket on Avro Format  
3) Simultaneously generate a log table with the path of the most recently written table  
  
NIFI doesn´t support writes on parquet or delta format, so this aproach emulates a Delta-like method, enabling PySpark scripts to identify the current table version between multiple batches.

### 3.2 Volumes for NIFI + MySQL + HDFS🔄

NIFI needs some dependencies to read from MySQL and write to HDFS.

Considering that the current [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:

          nifi:  
            volumes:  
              - ./nifi/lib:/opt/nifi/nifi-current/lib  
              - ./shared-data:/opt/nifi/shared-data  

The following Repository Files were mounted locally for the volumes to work:

        - "C:\docker\apache-stack\nifi\lib\mysql-connector-j-9.3.0.jar"  
        - "C:\docker\apache-stack\nifi\lib\nifi-hadoop-nar-2.4.0.nar"  
        - "C:\docker\apache-stack\nifi\lib\nifi-hadoop-libraries-nar-2.4.0.nar"  
        - "C:\docker\apache-stack\shared-data\core-site.xml"  
        - "C:\docker\apache-stack\shared-data\hdfs-site.xml"   

### 3.3 Creating Process Group and Controller Services🔄

To read MySQL and write tables on Avro format, the following Controller Services need to be added and enabled to the Process Group:  

1) AvroReader   
2) AvroRecordSetWriter   
3) DBCPConnectionPool

After confirming that nifi container was running, the NIFI web interface was accessed via https://localhost:8443/ 

The Controller Services were configured by creating a new Process Group > Entering the Process Group > Opening the Controller Services menu > Adding and enabling each Controller Service.

![nifi3](https://github.com/user-attachments/assets/2dc0a4a8-c567-4a79-bebc-9acc62ee1c4f)

![4](https://github.com/user-attachments/assets/964f5a42-1719-4f21-8042-6c73516af2c1)

![nifi7](https://github.com/user-attachments/assets/0f02aec3-fea0-481e-88b3-a62793ccd891)

![nifi6](https://github.com/user-attachments/assets/9b3f01e7-7696-4a39-a9b5-9922db5ba5ec)


### 3.4 Adding the Processors to the Process Group 🔄

To execute the reads and writes, the following Processors were added to the Process Group in order:

1) **GenerateFlowFile:** Triggers the pipeline with an empty file  
2) **UpdateAttribute:** Sets table name  
3) **ExecuteSQL:** Runs Query to extract the table from MySQL, uses the controller DBCPConnectionPool  
4) **UpdateAttribute:** Sets HDFS location paths  
5) **UpdateAttribute:** Names the write´s path and folder, with the execution timestamp  
6) **ConvertRecord:** Converts table to Avro format, uses the controller AvroReader and AvroRecordSetWriter  
7) **PutHDFS:** Writes Avro Table on the HDFS path  
8) **UpdateAttribute:** Sets filename for the log table  
9) **ReplaceText:** Generates log table, adding a row with the name and time of the last batch   
10) **PutHDFS:** Writes log table

![nifi8](https://github.com/user-attachments/assets/4ee2e2af-5ed8-4ba0-bc3f-f0a490f76c19)

(The Repository File [Process_Group_Sample.json](https://github.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/blob/main/Nifi%20Process%20Groups/Process%20Group%20Sample.json) it´s the same Process Group and can also be imported)

To read the 10 source tables, the Process group was replicated 10 times, each time configuring the properties of the 2nd Processor "**UpdateAttribute:**" with the table name.

![nifi99](https://github.com/user-attachments/assets/4bb94f55-f428-42b6-a5a8-1db0456dda1e)


### 3.4 Testing the Process Groups 🔄

The current Staging bucket on HDFS was empty:  

![hue vacia](https://github.com/user-attachments/assets/f5dd8149-81ab-4229-83e3-67575a8d0896)
The process group for the table "Zonas" was tested by clicking on the "play" button:  

![nifi play](https://github.com/user-attachments/assets/4a5880d9-958a-4939-bb5f-03ccbcab7297)  

After a few seconds, the folder for the table "Zonas" was created on HDFS:  

![hue vllena1](https://github.com/user-attachments/assets/6939b07f-5160-4926-aa6b-8f3205a58621)

Also the Avro Table and the log with the last write were created inside the folder:

![hue vllena2](https://github.com/user-attachments/assets/d2600fe2-0252-45c3-8f6f-f1bd5c037c4d)


The remaining Process Groups were tested as well by clicking "play"

![nifi91111](https://github.com/user-attachments/assets/103ac222-82ba-474f-9629-5cb78a223ad3)

A folder for each source table were created:

![nifi vllena34](https://github.com/user-attachments/assets/10d17948-4623-4d28-81e0-fc3187d995cf)


After testing out the execution, all source tables were properly written to HDFS, meaning the Process Groups are ready to be automated after. 

### 4. Apache Spark ⚡

### 4.1 Volumes for Spark and Python container ⚡

Apache Spark needs some dependencies to read Avro files and write tables in delta format.

Considering that the current [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:  

          python:
              volumes:
                - ./spark/jars-ext:/opt/delta-jars:ro

          spark:
              volumes:
                - ./spark/jars-ext:/opt/bitnami/spark/delta-jars

The following Repository Files were mounted locally for the volumes to work:

        - C:\docker\apache-stack\spark\jars-ext\spark-avro_2.12-3.5.0.jar
        - C:\docker\apache-stack\spark\jars-ext\delta-storage-3.2.0.jar
        - C:\docker\apache-stack\spark\jars-ext\delta-spark_2.12-3.2.0.jar

### 4.2 Ataching IDE to Container ⚡

To support script development, Visual Studio Code (VScode) was selected as the designated IDE.

VScode was atached to the Python container, for pyspark code testing. (Connecting to Spark Container was also an option, but volume mapping for Delta Lake JAR dependencies was easier with Python Container)

![vs1](https://github.com/user-attachments/assets/5947018b-9efc-404c-a822-8f742bb1f845)


### 4.3 Installing PySpark and Delta-Spark⚡

Considering that the Python Container for script testing and the Spark Container for script execution should have the same Delta-Spark and PySpark versions, the following commands were executed FOR BOTH CONTAINERS in order:

        pip install --no-cache-dir delta-spark==4.0.0
        pip install --no-cache-dir pyspark==3.5.1

(Can be done on terminal, vscode or from Dockerfile)  

NOTE: The installation of delta-spark may modify the pyspark version, which can be fixed by installing pyspark right after.

### 4.4 Developing Spark Job 1: Configure Spark⚡

Spark session was set-up with the following configurations:

To define local development resources (Execution or real cluster will use different settings)

    .master("local[*]") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "1g") \

To ensure all required libraries were available to the Python container:

    .config("spark.jars", 
        "/opt/delta-jars/delta-spark_2.12-3.2.0.jar,"
        "/opt/delta-jars/delta-storage-3.2.0.jar,"
        "/opt/delta-jars/spark-avro_2.12-3.5.0.jar")\
        
To enable read and write for Delta format tables:
          
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

To detect schema changes:   

    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \

To enable MERGE function for writing delta format tables

    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \

![vs2](https://github.com/user-attachments/assets/5d1fad65-f65d-44f0-9928-a523db4e74fb)

### 4.5 Developing Spark Job 1: Read from HDFS, Struct-type and Cast ⚡

The first read targets the log table to obtain the path of the last written table.

    table_path_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("hdfs://hadoop-namenode:9000/user/NameUser/MainFolder/StagingFolder/TableName/_last_write_TableName")
    table_path = table_path_df.first()["write_path"]

To avoid inference issues and improve Read performance, each column was explicitly defined using StructType specifying the types from the source table.

    table_schema = StructType([
        StructField("col_1", IntegerType(),True),
        StructField("col_2", StringType(), True),
    ])

Then the second read uses the path to target the last written table.

    table_df = spark.read.format("avro") \
        .schema(table_schema) \
        .load(table_path)
        
Then for the columns that require specific data types, casting was used to ensure appropriate types.

    casted_table_df = table_df\
        .withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss.S"))\
        .withColumn("purchaseDate", to_date("purchaseDate", "yyyy-MM-dd"))

![vs3](https://github.com/user-attachments/assets/cef1a88b-674f-4de0-b702-46a1e4ac5a87)

Using the same logic, that process was applied to each of the 10 tables stored in HDFS.

### 4.6 Developing Spark Job 1: Write tables in Delta Format into Bronze Layer ⚡

To allow incremental updates, change data capture (CDC) and avoid overwritting historical data, this custom function was defined to perform upserts (update/insert):

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

- If a Delta table exists, it merges the new table (df) with the existing Delta table in HDFS, updating matching records and inserting new ones.
- If the Delta table doesn't exist, it creates a new one by writing the table in overwrite mode.

Then every write was done for the 10 transformed tables, by calling the **upsert_to_delta()** function

    upsert_to_delta(
        transformed_df,
        "hdfs://hadoop-namenode:9000/user/NameUser/MainFolder/BronzeFolder/DeltaTableName",
        "target.id_column = source.id_column"
    )
    
![vs5](https://github.com/user-attachments/assets/daacd3cd-52ad-4f5f-839c-8658140f166f)

### 4.7 Testing Spark Job 1 ⚡

The script was tested via VsCode atached to Python Container with Pyspark and Delta-Spark installed

![vs6](https://github.com/user-attachments/assets/d05d4f86-05c4-416f-b327-54b2fea51376)

The results were validated by the appearance of folders on HDFS, inside the Bronze layer directory:

![hue3](https://github.com/user-attachments/assets/8f032f0d-5167-4623-9ddb-3be01d30df74)

The Delta tables were also successfully written to their respective folders:

![hue4](https://github.com/user-attachments/assets/1e830042-b03d-4162-82e0-c563c3a42fa9)

### 4.8 Developing Spark Job 2: Configure Spark⚡

A new script was created. Configurations used in Spark Job 1 were maintained, with only the imported modules and functions varying between jobs.

![VS7](https://github.com/user-attachments/assets/a9506ef0-0766-465f-b988-9ed084bb4be3)

### 4.9 Developing Spark Job 2: Read Bronze Tables⚡

The Bronze Tables are in delta format, so the reads target directly to them.

![vs8](https://github.com/user-attachments/assets/08beeb6f-d7ea-47b8-a8f1-e891fceee847)

### 4.10 Developing Spark Job 2: Transform Tables⚡

The following transformations were implemented:
- Selecting only the necessary columns for upcoming joins
- Filtering by Active Status and updates from the current year
- Cleaning anything needed to get the filter right
- Treating nulls for tables that require it
- Creating new required columns that don´t rely on external tables or aggregations yet

![vs9](https://github.com/user-attachments/assets/410961d1-e701-4594-95c4-202dc3ac820b)

### 4.11 Developing Spark Job 2: Write Silver Tables⚡

The upsert method used in Spark Job 1 was maintained and applied to write each table into the Silver Layer:

![vs91](https://github.com/user-attachments/assets/eba53a2c-14c5-4d81-b220-f25c5b9f3cd8)

### 4.12 Testing Spark Job 2 ⚡

The script was tested via VsCode atached to Python Container with Pyspark and Delta-Spark installed

![vs92](https://github.com/user-attachments/assets/b766d688-d33a-4076-bc6f-455f70cafdaa)

The results were validated by the appearance of folders and Delta tables on HDFS, inside the Silver layer directory:

![hue91](https://github.com/user-attachments/assets/1954546d-165e-4232-a6db-4d0f0d9e8660)

### 4.13 Developing Spark Job 3: Configure Spark⚡

A new script was created. Configuration used on previous Jobs were maintained, with only the imported modules and functions varying between jobs.

![VS93](https://github.com/user-attachments/assets/d4ccaf25-6994-4cb5-b501-a946b1cecc64)

### 4.14 Developing Spark Job 2: Read Silver Tables⚡
The Silver Tables are in delta format, so the reads target directly to them.

![vs94](https://github.com/user-attachments/assets/f09d6b1c-93b1-482a-aade-de1b67abdd8a)

### 4.15 Developing Spark Job 3: Join Silver and Aggregated Tables ⚡

To create a denormalized final table, the silver tables were joined by their primary key pulling out their description and required attribute columns:

![vs95](https://github.com/user-attachments/assets/dd0537b7-d0b6-4658-9dbd-5cf35c7c49dc)

An aggregated table was created from the Sales Table (Ventas) by grouping sales data by distributor ID (id_distribuidor) and calculating the earliest sale date (fechaVenta) as the activation date (FechaActivacion):

![vs96](https://github.com/user-attachments/assets/de03c68e-b62e-453b-ac55-89560e70dd88)

### 4.16 Developing Spark Job 3: Write Gold Table⚡

A column renaming was done, following by calling the upsert function to write the final table:

![vs96](https://github.com/user-attachments/assets/14e9f5a3-9953-4c1b-a56b-3c70817cdc19)

### 4.17 Testing Spark Job 3 ⚡

The script was tested via VsCode atached to Python Container with Pyspark and Delta-Spark installed

![vs97](https://github.com/user-attachments/assets/787495e7-3cc4-4fe9-a5a6-dba6ca762405)


The results were validated by the appearance of the folder and Delta table on HDFS, inside the Gold layer directory:

![hue92](https://github.com/user-attachments/assets/5cdbd27f-c509-45fb-afad-3809d5ea9f2a)

### 4.18 Executing Jobs on Spark Container using Spark-Submit ⚡

The 3 scripts were exported and considering that the current [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:  

    spark:
        volumes:
          - ./shared-data:/opt/bitnami/spark/shared-data

The Spark Jobs scripts were mounted locally for the volumes to work:

    C:\docker\apache-stack\shared-data\Py-Scripts\SPARKJOB1.py
    C:\docker\apache-stack\shared-data\Py-Scripts\SPARKJOB2.py
    C:\docker\apache-stack\shared-data\Py-Scripts\SPARKJOB3.py


Using the Spark Container´s user, the following command was executed from terminal:

    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark:7077 \
    --jars "/opt/bitnami/spark/delta-jars/delta-spark_2.12-3.2.0.jar,\
            /opt/bitnami/spark/delta-jars/delta-storage-3.2.0.jar,\
            /opt/bitnami/spark/delta-jars/spark-avro_2.12-3.5.0.jar" \
    /opt/bitnami/spark/shared-data/Py-Scripts/SPARKJOB1.py

NOTE: Now the Spark Container was selected to run the job on Cluster Standalone mode.

This operation was done for Spark Job 2 and 3 as well. The results were validated by the appearance of a new batch of tables in HDFS:

![hueya](https://github.com/user-attachments/assets/2f486b84-1891-4ce9-8fcd-6f6e83634e34)


### 5. Apache Airflow ⏱️

### 5. Setting-up NiFi+Airflow comunication  ⏱️

NiFi can be automated via REST API requests, but by default, it only listens on localhost. In a containerized environment, applications communicate using the container's name instead of localhost.

So, the NiFi TLS/SSL certificate's common name (CN=localhost) was changed (CN=nifi) by the following actions:

To get the keystore´s password, this command was executed from Terminal on Nifi Container´s User: 

    /opt/nifi/nifi-current$ cat ./conf/nifi.properties | grep "nifi.security.keystore"

This command was executed from C:\docker\apache-stack\nifi\certs> (Using the obtained password on storepass and keypass)

    keytool -genkeypair \
      -alias generated \
      -keyalg RSA \
      -keysize 4096 \
      -keystore keystore.p12 \
      -validity 60 \
      -storetype PKCS12 \
      -dname "CN=nifi" \
      -storepass <PASSWORD> \
      -keypass <PASSWORD> \
      -ext "SAN=DNS:nifi,DNS:localhost,DNS:nifi-container,IP:127.0.0.1" \
      -ext "KU=digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment,keyAgreement,keyCertSign,crlSign" \
      -ext "EKU=clientAuth,serverAuth" \
      -ext "BC=ca:true"

Now the following file was mounted locally:

    C:\docker\apache-stack\nifi\certs\keystore.p12
  
Validation was done by running the following command from Airflow Container´s Root :

    openssl s_client -connect nifi:8443 -showcerts

![cmdairflow](https://github.com/user-attachments/assets/8ed12669-0aba-4810-a396-dc9d0ad46bc6)

Now the NiFi Container will listen to Airflow Container´s requests.

### 5. Testing a NiFi Process Group´s execution via request ⏱️

NiFi uses JWT authentication, so a Token was requested via Terminal from the Airflow´s root user. (The same NiFi´s interface Password its required)

    curl -k -X POST https://nifi:8443/nifi-api/access/token -d 'username=admin&password=<PASSWORD>'

The following elements were obtained:  
The TOKEN requested
![nifi92](https://github.com/user-attachments/assets/83cd068d-a52a-4d06-84d0-be0cb5ccfce5)

The Process Group´s ID
![nifi93](https://github.com/user-attachments/assets/9cb4781e-6eee-4ce8-96a4-c12488b94806)


To execute a Process Group from request, the following command was used with the TOKEN and the ID:

          curl -k -X PUT \
            https://nifi:8443/nifi-api/flow/process-groups/<ID> \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer <TOKEN>" \
            -d '{
            "id": "<ID>",
              "state": "RUNNING",
              "component": {
                "id": "<ID>",
                "state": "RUNNING"
              },
              "revision": {
                "clientId": "curl-cmd"
              }
            }'
            
The results were validated by the "RUNNING" status via Terminal and Interface:

![NIFIEX](https://github.com/user-attachments/assets/c22be9e9-510a-41ec-af11-ac28dafd11e2)
![NIFIEX1](https://github.com/user-attachments/assets/3a94cb47-4742-4420-8d12-e2abf196a32e)

Then the process group was manually stopped, this was only a test to prove everything´s set up for upcoming automation with Airflow DAGs.  

### 5. Volumes for Airflow Containers ⏱️

Considering that the current [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:

          airflow:
              volumes:
                  - ./airflow/spark-scripts:/opt/airflow/spark-scripts  
                  - ./airflow/spark-jars:/opt/airflow/spark-jars
                  - /var/run/docker.sock:/var/run/docker.sock
          airflow-scheduler:
              volumes:
                  - ./airflow/spark-scripts:/opt/airflow/spark-scripts  
                  - ./airflow/spark-jars:/opt/airflow/spark-jars

The following files were mounted locally for the volumes to work:

          "C:\docker\apache-stack\airflow\spark-jars\spark-avro_2.12-3.5.0.jar"
          "C:\docker\apache-stack\airflow\spark-jars\delta-storage-3.2.0.jar
          "C:\docker\apache-stack\airflow\spark-jars\delta-spark_2.12-3.2.0.jar
          "C:\docker\apache-stack\airflow\spark-scripts\SPARKJOB1.py"
          "C:\docker\apache-stack\airflow\spark-scripts\SPARKJOB2.py"
          "C:\docker\apache-stack\airflow\spark-scripts\SPARKJOB3.py"

### 5. Installing Spark, Delta-Spark and HDFS for Airflow Containers ⏱️

To use SparkSubmitOperator(), call the jobs that use Delta and create Custom Sensors for HDFS folders, Airflow need some dependencies.

Since the initial composing of containers, the [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) should have already installed Pyspark and Delta-Spark on Airflow and Airflow Scheduler containers using the [Dockerfile](https://github.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/blob/main/Docker%20Setup/Dockerfile)

If for some reason it´s not, then it can be done from terminal for BOTH AIRFLOW and AIRFLOW SCHEDULER CONTAINERS in order:

        pip install --no-cache-dir delta-spark==4.0.0
        pip install --no-cache-dir pyspark==3.5.1 
        pip install apache-airflow-providers-apache-hdfs --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
        pip install apache-airflow-providers-apache-spark --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

NOTE: The installation of delta-spark may modify the pyspark version, which can be fixed by installing pyspark right after.

### 5. Creating HDFS and Spark connections ⏱️

A HDFS connection was configured by running the following command with Airflow User:

          airflow connections add \
              --conn-type webhdfs \
              --conn-host hadoop-namenode \
              --conn-port 9870 \
              --conn-extra '{"proxy_user": "usuario"}' \
              webhdfs_default || true

Also a Spark connection was configured by running the following command with Airflow User:

          airflow connections add \
              --conn-type spark \
              --conn-host spark://spark \
              --conn-port 7077 \
              spark-conn || true

(The [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) should have already created the connections using the [Dockerfile](https://github.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/blob/main/Docker%20Setup/Dockerfile)

### 5. Storing Airflow Secret Variables ⏱️

To avoid hardcoding credentials on the DAG, the ID´s of the 10 Process Groups and the Access Password were stored on Airflow´s Variables via [localhost:8082](http://localhost:8082/)  

![AW1](https://github.com/user-attachments/assets/b1a89e09-6b3d-4850-96fe-2e7316c01c84)


### 5. Developing DAG 1: NiFi Orchestration ⏱️

A new script file was created. The DAG was configurated considering that the end-user needs the data to be updated daily at 8:00am:

![AW2](https://github.com/user-attachments/assets/4cf6edb0-f7a0-4fb6-ad72-1b1d8a2a005e)


To add a sensor that detects when a new file appeares inside the HDFS folder, the following custom sensor was defined using BaseSensorOperator and the webHDFS connection

![aws3](https://github.com/user-attachments/assets/3173cf13-5a30-4ef9-9c55-15574230ef1f)

To get a new token each time the DAG requests NiFi to start Process Groups, the following operator was created, using xcom to apply the token on upcoming operators:  

![aw4](https://github.com/user-attachments/assets/01a9155f-deb5-4259-a6bf-4ea07bf61764)

Using **var.value.get()** to get the ID from Airflow´s Variables, and **ti.xcom_pull()** to use the output token, the following **BashOperator()** was created to start a NiFi Process Group:

![aw4](https://github.com/user-attachments/assets/49bcbf45-b492-4d48-b9a8-648e92c16efe)

The following sensor was created using the custom class we defined earlier, to success if a file appears on the HDFS path or fail if it doesn´t:

![aw5](https://github.com/user-attachments/assets/53ae30a9-c0d5-4e57-bdf2-2de32f1160e0)

Mirroring the start_pg Operator, the following one was created to stop the NiFi Process Group:

![aws4](https://github.com/user-attachments/assets/c560dde3-c1c6-430d-8852-2c405f520de2)

(Using the same logic, the same Operators and Sensor were defined for each of the 10 Process Groups that Read MySQL and Write on HDFS)

Finnally, a TriggerDagRunOperator() was added to begin the Spark Dag after all the ProcessGroup were Stopped, with the following the Dag Flow definition:

![aws5](https://github.com/user-attachments/assets/79e0d519-9c8b-4e26-aa10-896ddac4eae9)

### 5. Developing DAG 2: Spark Orchestration ⏱️

A new script file was created for DAG 2. This time considering no schedule because the DAG 1 it´s going to trigger it.
![AWS7](https://github.com/user-attachments/assets/56ae04d9-483c-4831-adc2-4dd1d4221789)

A SparkSubmitOperator() was defined for each SparkJob, adding a wait of 30 seconds with a PythonOperator() between jobs:

![AWS7](https://github.com/user-attachments/assets/d416d776-8a2b-4899-b8ac-6c13325882d6)

Finally this simple execution order was defined:

![AWS7](https://github.com/user-attachments/assets/d0af3094-4ad9-490f-ad1d-a9cae6545c0b)

### 5. Running Airflow DAGs ⏱️

The python scripts from both DAGs were placed on the following local files:

    C:\docker\apache-stack\airflow\dags\NIFI_DAG.py
    C:\docker\apache-stack\airflow\dags\SPARK_SUBMIT_DAG.py

The DAGs then appeared on the list via Airflow web interface and the DAG 1 was triggered with the play button:

![AWS7](https://github.com/user-attachments/assets/8e26e7df-0080-4870-a5b9-35365d2597aa)

The results were informed on the Graph sections:

![aws8](https://github.com/user-attachments/assets/404d83d3-9017-4ad4-beb4-7e15d5e0447d)  

![aws9](https://github.com/user-attachments/assets/6aaf337a-8809-478e-9b5b-3e9e5a1183dc)

And of course, the results were validated by the appearence of a new Batch of delta tables on the Bronze, Silver and Gold layer:

![aws9](https://github.com/user-attachments/assets/2e46ff38-272b-405c-bb72-1c17ee9217a4)

### 5. Testing Alerts for the Airflow DAGs ⏱️

For testing purposes, an incorrect file name was intentionally added to the following sensor:  

![AW91](https://github.com/user-attachments/assets/2c5f0a1a-dbc5-41d1-b5bd-d1ce84140f33)

The DAGs were triggered and that sensor failed after some retries:

![aws92](https://github.com/user-attachments/assets/2ca5b5aa-c3aa-47d0-ba6e-ef47b61635da)

And the alert was sended to the email specified on the docker-compose

    airflow:
      environment:
        - AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
        - AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
        - AIRFLOW__SMTP__SMTP_MAIL_FROM=*****@gmail.com
        - AIRFLOW__SMTP__SMTP_USER=*****@gmail.com
        - AIRFLOW__SMTP__SMTP_PASSWORD=*****
        
![as93](https://github.com/user-attachments/assets/8717e498-0a84-4be3-89a1-6d670acc6a61)


### Learnings

![funcionesusadas](https://github.com/user-attachments/assets/ff5af71f-5a19-4f68-8aad-a2940089bc55)

### Spanish/English Glossary

Afiliación = Affiliation  
Apellido (primer/segundo) = Last Name (first/second)  
Asignación = Assignment  
Cantidad = Quantity  
Cliente = Client / Customer  
Comentario = Comment  
Correo = Email  
Código Postal = Postal Code / Zip Code  
Curp = CURP (Mexican ID)  
Descripción = Description  
Dirección = Address  
Distribuidor = Distributor  
Empleado = Employee  
Estatus = Status  
Evaluación = Evaluation  
Fecha = Date  
Fecha Alta = Registration Date  
Fecha Movimiento = Movement Date  
Fecha Venta = Sale Date  
Historial de Crédito = Credit History  
Motivo de No Interesado = Not Interested Reason  
Motivo de Rechazo = Rejection Reason  
Nombre = Name  
NSS = NSS (Social Security Number)  
Origen = Source  
Perfil = Profile  
Precio = Price  
Producto = Product  
Prospecto = Prospect  
Puesto = Position / Job Role  
Región = Region  
Referido = Referral  
RFC = RFC (Tax ID)  
Salario = Salary  
Sucursal = Branch  
Situación = Situation / Status  
Teléfono = Phone  
Usuario = User  
Venta = Sale  
Zona = Zone  

