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

### -1.1 Volumes for Docker 🐳
- Get Docker Desktop app, and name your proyect on the docker folder. 
For example: I named it "apache-stack" and my path was "C:\docker\apache-stack"

- Get the repository files, then put [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) and the [Dockerfile](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/Dockerfile) inside the path.
- The following volumes should exist in the local PC with the Repository files: 
"C:\docker\apache-stack\docker-compose.yml"  
"C:\docker\apache-stack\Dockerfile"

### -1.2 Build-up Containers 🐳

- On CMD, run "docker-compose up -d" from the "C:\docker\apache-stack" path.

- On Docker Desktop, your containers should appear like this:

![docker1](https://github.com/user-attachments/assets/392775c9-191b-4b5a-8eda-e26786b2bc0d)


### 2. Apache HDFS 🗂️

### -2.1 Volumes for Hadoop User Experience (HUE) 🗂️

- Consider that the actual [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:  
  hue:  
    volumes:  
      - ./shared-data/hue.ini:/usr/share/hue/desktop/conf/hue.ini  
      - ./shared-data/hue-data:/hue  

- The following volume should exist in the local PC with the Repository file:  
"C:\docker\apache-stack\shared-data\hue.ini"

### -2.2 Create Medallion Folders in HDFS using HUE🗂️

- Make sure hue, hadoop-datanode, hadoop-namenode containers are Running.

- Go to http://localhost:8888/ to access Hue´s web interface. If first time, put any user and password but remember them.

- Clic on Files > New > Directory 

![hue1](https://github.com/user-attachments/assets/d8e58d82-68f5-4494-98e1-0c6afeaa7df2)

- Name your main folder. (Consider that this names are gonna be used on the python scripts)
![hue1](https://github.com/user-attachments/assets/7c1fd2c6-ca6f-40e5-aeb0-8969cb6065c7)

- Inside the main folder, create and name 4 new folders, 3 with a medallion hierarchy and 1 for staging or landing. It should look like this:

![hue2](https://github.com/user-attachments/assets/11a6d07c-b0d1-4e66-926b-f9a968c2857a)


### 3. Apache NIFI 🔄

### -3.1 Volumes for NIFI + MySQL + HDFS🔄

NIFI needs some dependencies to read from MySQL and write to HDFS.

- Consider that the actual [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) created this volumes:  
  nifi:  
    volumes:  
      - ./nifi/lib:/opt/nifi/nifi-current/lib  
      - ./shared-data:/opt/nifi/shared-data  

- The following volumes should exist in the local PC with the Repository files:  
"C:\docker\apache-stack\nifi\lib\mysql-connector-j-9.3.0.jar"  
"C:\docker\apache-stack\nifi\lib\nifi-hadoop-nar-2.4.0.nar"  
"C:\docker\apache-stack\nifi\lib\nifi-hadoop-libraries-nar-2.4.0.nar"  
"C:\docker\apache-stack\shared-data\core-site.xml"  
"C:\docker\apache-stack\shared-data\hdfs-site.xml"   

### -3.2 File Volumes for NIFI + MySQL + HDFS🔄



### Spark:

### Glossary

### Learnings

![funcionesusadas](https://github.com/user-attachments/assets/ff5af71f-5a19-4f68-8aad-a2940089bc55)

