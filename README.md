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
### 1. Docker Container 🐳

### --1.1 File Volumes for Docker 🐳
- Get Docker Desktop app, and name your proyect on the docker folder. 
For example: I named it "apache-stack" and my path was "C:\docker\apache-stack"

- Put the [docker-compose.yml](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/docker-compose.yml) and the [Dockerfile](https://raw.githubusercontent.com/arinrohega/DE01-Pipeline01-ApacheStack-DeltaLake/refs/heads/main/Docker%20Setup/Dockerfile) inside the path.
- You should now have the following volumes:  
"C:\docker\apache-stack\docker-compose.yml"  
"C:\docker\apache-stack\Dockerfile"

### --1.2 Build-up Containers 🐳

- On CMD, run "docker-compose up -d" from the "C:\docker\apache-stack" path.

- On Docker Desktop, your containers should appear like this:
![docker1](https://github.com/user-attachments/assets/392775c9-191b-4b5a-8eda-e26786b2bc0d)




### 2. Apache HDFS 🗂️

### --2.1 Create Medallion Folders 🗂️
sdfgsdfg

### Spark:

### Glossary

### Learnings

![funcionesusadas](https://github.com/user-attachments/assets/ff5af71f-5a19-4f68-8aad-a2940089bc55)

