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

The Repository File [Process_Group_Sample.json](https://github.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/blob/main/Nifi%20Process%20Groups/Process%20Group%20Sample.json) it´s the same Process Group and can also be imported)

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







### Spark:

### Glossary

### Learnings

![funcionesusadas](https://github.com/user-attachments/assets/ff5af71f-5a19-4f68-8aad-a2940089bc55)

