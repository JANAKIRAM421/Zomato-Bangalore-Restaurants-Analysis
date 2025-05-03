######################################################## Zomato Bangalore Restaurants Analysis ##############################################################################

Overview
This project analyzes Zomato restaurant data from Bangalore to understand dining preferences, ratings, and pricing trends. Using Python, SQL, Hadoop FS, Hive, PySpark, and Google BigQuery, we process and analyze restaurant data to provide insights for restaurateurs and food platforms.

Dataset
The dataset, sourced from Kaggle, contains restaurant details with columns like Restaurant_ID, Name, Location, Cuisine, Rating, and Price_Range.

Workflow
1.	Data Ingestion with Hadoop FS:
–	Use Python to download the dataset and upload it to Hadoop Distributed File System (HDFS) for scalable storage.
2.	Data Storage and Schema Definition with Hive:
–	Create a Hive external table, defining the schema (e.g., Restaurant_ID as STRING, Rating as FLOAT, Price_Range as STRING).
–	Use HiveQL to load data from HDFS into the table.
3.	Data Processing with PySpark:
–	Use PySpark to clean data, handling missing values, standardizing cuisine types, and removing duplicates.
–	Perform aggregations to calculate average ratings by cuisine and location.
4.	Advanced Analytics with BigQuery:
–	Export processed data to Google BigQuery for advanced SQL-based analytics.
–	Use Python’s google-cloud-bigquery library to load data into a BigQuery table.
–	Run SQL queries to analyze rating trends by price range and location.
5.	Visualization with Python:
–	Use Matplotlib and Seaborn to visualize results, such as rating distributions or cuisine popularity.

Outcomes
•	Identify high-rated cuisines and locations.
•	Highlight pricing trends and their impact on ratings.
•	Provide insights for restaurant marketing and menu planning.

Tools
•	Python: Data ingestion, preprocessing, and visualization.
•	SQL: Querying in Hive and BigQuery for analytics.
•	Hadoop FS: Scalable storage for raw data.
•	Hive: Structured data management and initial querying.
•	PySpark: Large-scale data processing and transformation.
•	BigQuery: Cloud-based analytics for complex queries.
