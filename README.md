
![Arquitetura](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/gcp-architecture.png)


This project was developed due to a challenge, which made data sources available and requested a solution within a cloud infrastructure.
The database provided provides the context of a company that produces fictitious bicycles.

TO DO:

## -Perform data modeling;
## -Creation of cloud infrastructure
## -Creation of all artifacts necessary to load files into the database;
## -Development of scripts for data analysis;
## -optional: create a report with any BI tool.

For the development of the project, the Google platform - GCP was used as it is the cloud in which I wish to specialize. However, it was a big challenge as it was my first contact in developing an "end to end" project, as I currently work with AWS and had already used Azure Databricks.

Considering a pilot project that has room for improvements that will be listed at the end of this readme. The initial proposal was to use much of the resource creation through Python programming, using the official GCP API. Of the resources used we have:

  **Google Cloud Storage**: Buckets to store already cleaned csv files (after treatment by notebooks.ipynb);

  **Google Big Quey**: The idea of using GCP big query was because it is a serverless service and a Modern Data Warehouse with a lot of processing power, a lot of potential with the next updates already announced (DuetAI, notebooks, etc.) and its relatively low cost (caution when using). In addition to facilitating Ad-hoc queries and having a native connection with Looker Studio, which was our choice of BI tool;
 
  **Google Looker Studio**: Since the entire project is already being developed at GCP, there is nothing better than using the provider's own BI tool, facilitating connections and avoiding problems in exporting data, creating dashboards and structuring relationships between tables. Which, by the way, was very intuitive in this tool.

  **Google IAM**: Here we define the API access keys that were used in the Python scripts that create the DataBase, create the tables using DDL's, and load data from Google Storage for each respective table.

  **PySpark**: The data processing and table cleaning scripts were created in PySpark, already thinking about a Big Data scenario where we could use a **GCP DataProc**, to have a cluster and carry out these treatments without having to re-write the code.

  **drawio**: used to create the illustration of the architecture used.


  ### Use of Scripts:
 to use the ".py" scripts from the folder [Scripts](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/scripts). It is necessary that you export the environment variables, and especially your credentials.json (a valuable tip, set aside time for the credentials to be deactivated or delete them manually after use). Examples of environment variables:


export  MY_GCS_BUCKET_URL = "<yourbucket_url>"

export  GOOGLE_APPLICATION_CREDENTIALS="<your_credentials.json>"

export DDL_BUCKET = "<your_credentials.json>"  # This bucket is addressed for future updates **GCP Functions**

export DATA_BUCKET = "<yourbucket_name>" 

export DATASET_NAME = "<yourDataSetName>"

**Note: a virtualenv was used for this project and the libs used are in requirements.txt**

### Data Cleaning:

Before starting to load the data and carry out all the analyses, it was necessary to take care of each file that makes up the database. As previously mentioned, **PySpark** was used for these treatments and can be used in a BigData scenario in a **DataProc cluster** if necessary. These files are in the folder: [scripts/ipynbs_data_cleaning](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/scripts/ipynbs_data_cleaning).

Important topics to be considerd:
- The schema of each table was "forced" to have congruence between the DDL and the data exported by Spark and thus not have conflicts when loading GCP big query;
- The csv separator was changed from ";" for "," although Big query supports changing the delimiter, it was decided to leave a default that most projects use;
- The time format used in all tables was classified as **TimeStamp** because the big query can bring each date in greater detail in relation to **datetime**;
- Texts with string "null" were removed with regex;
- Customer table, the PersonID column was set to integer, it was one of the problems that it was initially seen as a string, the relationships did not occur in the queries, it was fixed in "mid-development".
- Primary key IDs from the **SpecialOfferProduct" table were deduplicated.


### Dimensional modeling

Topology of data sent:

[Relacionamento_tabelas](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/relacionamento_tabelas.png)

Our "DB" which in bigquery is called "DataSet" but which works analogously to DB. The structure was defined as follows:

- rox-test-task
  - testRoxDB
    - customer_personid_not_null
    - Person
    - Product
    - SalesOrderDetail
    - SalesOrderHeader
    - SpecialOfferProduct

The DDLs used in the table creation script directly in big query can be consulted in the following folder: [DDLs.sql](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/local_csv_data/tabel_definitions_DDL) and for the creation of the big query DB/dataSet that will house these tables: [DB_dataset](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/local_csv_data/db_definition)

With these files and the scripts: [create_tables](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/create_table_from_DDL.py) and [create_DB_dataset](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/create_dataset_db_bq.py) It was possible to create the tables and the DD/Dataset respectively in bigquery.


### ETL

Data ingestion was done using the python script: [migrate_csv_to_gcp](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/migrate_csv_to_gcp.py). The Script takes local files and loads them into gcp storage. It is worth mentioning that the files that were migrated from the local to the cloud are the files already processed resulting from the Jupyter notebooks.

Since we have from the previous step, the DB and the tables created (with schema defined by DDL.sql but empty at first) it was possible to use the script [insert_data_into_tables](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/insert_data_into_tables.py) to load the data from each clean csv to each respective table.

This way we already have data populating the tables and ready for the **Data Analysis** stage.

### Data Analysis - Answering business questions

1. Write a query that returns the number of rows in the Sales.SalesOrderDetail table by the SalesOrderID field, as long as they have at least three rows of details.

```
SELECT 
	SalesOrderID AS id, 
	COUNT(*) AS lines_qtd 
FROM `rox-test-task.testRoxDB.SaleOrderDetails`  as sod
GROUP BY SalesOrderID
HAVING lines_qtd >= 3
ORDER BY lines_qtd DESC
```

![query_1](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_1.png)

2. Write a query that links the Sales.SalesOrderDetail, Sales.SpecialOfferProduct and Production.Product tables and returns the 3 best-selling products (Name) (by the sum of OrderQty), grouped by the number of days to manufacture (DaysToManufacture).
```
WITH RankedProducts AS (
  SELECT
      p.Name AS ProductName,
      p.DaysToManufacture AS DaysToManufacture,
      SUM(sod.OrderQty) AS TotalQuantitySold,
      ROW_NUMBER() OVER(PARTITION BY p.DaysToManufacture ORDER BY sum(sod.OrderQty) DESC) as pos,
  FROM `rox-test-task.testRoxDB.SpecialOfferProduct` AS sop 
  JOIN `rox-test-task.testRoxDB.products` AS p ON sop.ProductID = p.ProductID
  JOIN `rox-test-task.testRoxDB.SaleOrderDetails` AS sod ON sop.SpecialOfferID = sod.SalesOrderDetailID
  GROUP BY p.Name, p.DaysToManufacture
)
SELECT * FROM RankedProducts WHERE pos <= 3
```
![query_2](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_2.png)

3. Write a query linking the Person.Person, Sales.Customer and Sales.SalesOrderHeader tables to obtain a list of customer names and a count of orders placed.

```
SELECT
	c.CustomerID AS id,
	CONCAT(p.FirstName, ' ', p.LastName) AS full_name, 
	COUNT(*) AS qtd
FROM `rox-test-task.testRoxDB.SalesOrderHeader` soh
JOIN `rox-test-task.testRoxDB.customer_personid_not_null` c ON soh.CustomerID = c.CustomerID
JOIN `rox-test-task.testRoxDB.person`  p ON c.PersonID = p.BusinessEntityID 
GROUP BY c.CustomerID, full_name
ORDER BY qtd DESC;
```

![query_3](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_3.png)

4. Write a query using the Sales.SalesOrderHeader, Sales.SalesOrderDetail and Production.Product tables, in order to obtain the total sum of products (OrderQty) by ProductID and OrderDate.

```
SELECT
    sod.ProductID AS id,
    p.Name AS nome,
    SUM(sod.OrderQty) AS qtd,
    soh.OrderDate AS order_date
FROM `rox-test-task.testRoxDB.SaleOrderDetails` sod
JOIN `rox-test-task.testRoxDB.SalesOrderHeader` soh ON sod.SalesOrderID = soh.SalesOrderID
JOIN `rox-test-task.testRoxDB.products` p ON sod.ProductID = p.ProductID
GROUP BY id, nome, order_date
ORDER BY order_date, qtd DESC;
```

![query_4](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_4.png)

5. Write a query showing the SalesOrderID, OrderDate and TotalDue fields from the Sales.SalesOrderHeader table. Get only the lines where the order was placed during the month of September/2011 and the total due is above 1,000. Sort by descending total due.

```
SELECT 
	SalesOrderID as id,
	CAST(OrderDate AS DATE) AS date, 
	TotalDue AS total_devido
FROM `rox-test-task.testRoxDB.SalesOrderHeader`
WHERE OrderDate BETWEEN '2011-09-01' AND '2011-09-30' AND TotalDue > 1000
ORDER BY total_devido DESC;
```


![query_5](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_5.png)

**Evidence for result of question 5.**


```
SELECT 
	SalesOrderID as id,
	CAST(OrderDate AS DATE) AS date, 
	TotalDue AS total_devido
FROM `rox-test-task.testRoxDB.SalesOrderHeader`
WHERE OrderDate BETWEEN '2011-08-01' AND '2011-10-01' AND TotalDue > 1000
ORDER BY date;
```

![evidencia_query_5](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_5_evidencia.png)


### Dashboard - BI

The native GCP tool was used for data visualization, Google Looker Studio (formerly Google Data Studio)

Table relationship:

![relacionamento_tabelas](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/relacionamento_looker_studio.png)

Dashboard is composed by two graphics:
  -Number of lost by date
  -Total by CustomerID (cumulative and in logarithmic scale)


![dashboard]((https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/relacionamento_looker_studio.png))


## Radar improvements:

- Immediately the first improvement is to place a GCP Function that will have the Trigger configured so that it is triggered whenever a new file enters the storage and carries out the incremental load on the corresponding table;

- Explore a non-serverless relational database GCP service for a Big Data scenario and compare costs with the direct bigquery option initially addressed in this project;

- use the Big Data scenario: increase the size of CSV files, and use the GCP cluster or **Data Proc**

- Use "Pydantic" to add value to our project in order to verify the schemas from our dataframes and destination tables before trying to load data into those Big Query tables.