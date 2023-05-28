from kafka import KafkaConsumer

# Configure Kafka consumer
bootstrap_servers = 'localhost:***'  
topic = 'clickstream_topic'  

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: x.decode('utf-8')  
)

# Extract and process clickstream data
for message in consumer:
    clickstream_event = message.value  

    # Extract required fields from clickstream event

    row_key = clickstream_event['row_key']
    user_id = clickstream_event['user_id']
    timestamp = clickstream_event['timestamp']
    url = clickstream_event['url']
    country = clickstream_event['country']
    city = clickstream_event['city']
    browser = clickstream_event['browser']
    operating_system = clickstream_event['operating_system']
    device = clickstream_event['device']

consumer.close()

import mysql.connector

# Configure MySQL connection
host = 'localhost'  
port = 3306  
username = 'username'  
password = 'password'  
database = 'database'  

# Establish MySQL connection

conn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

# Define the table schemas

click_data_table = """

CREATE TABLE click_data (
    row_key VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    timestamp DATETIME,
    url VARCHAR(255)
)
"""

geo_data_table = """

CREATE TABLE geo_data (
    row_key VARCHAR(255) PRIMARY KEY,
    country VARCHAR(255),
    city VARCHAR(255)
)
"""

user_agent_data_table = """

CREATE TABLE user_agent_data (
    row_key VARCHAR(255) PRIMARY KEY,
    browser VARCHAR(255),
    operating_system VARCHAR(255),
    device VARCHAR(255)
)
"""

# Execute table creation queries
cursor = conn.cursor()
cursor.execute(click_data_table)
cursor.execute(geo_data_table)
cursor.execute(user_agent_data_table)

# Store extracted fields in respective tables
click_data_insert = """
INSERT INTO click_data (row_key, user_id, timestamp, url)
VALUES (%s, %s, %s, %s)
"""

geo_data_insert = """
INSERT INTO geo_data (row_key, country, city)
VALUES (%s, %s, %s)
"""

user_agent_data_insert = """
INSERT INTO user_agent_data (row_key, browser, operating_system, device)
VALUES (%s, %s, %s, %s)
"""

# Example: Storing extracted fields in click_data table
click_data_values = (row_key, user_id, timestamp, url)
cursor.execute(click_data_insert, click_data_values)

# Example: Storing extracted fields in geo_data table
geo_data_values = (row_key, country, city)
cursor.execute(geo_data_insert, geo_data_values)

# Example: Storing extracted fields in user_agent_data table
user_agent_data_values = (row_key, browser, operating_system, device)
cursor.execute(user_agent_data_insert, user_agent_data_values)

# Commit the changes
conn.commit()

# Close the database connection gracefully when done
cursor.close()
conn.close()

import mysql.connector

# Establish MySQL connection (similar to previous code)
conn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

# Retrieve clickstream data from the click_data and geo_data tables
query = """
SELECT c.url, g.country, c.timestamp, c.user_id
FROM click_data c
JOIN geo_data g ON c.row_key = g.row_key
"""

cursor = conn.cursor()
cursor.execute(query)

# Fetch all rows of data
clickstream_data = cursor.fetchall()

# Close the cursor
cursor.close()

# Perform aggregations
aggregated_data = {}

for row in clickstream_data:
    url = row[0]
    country = row[1]
    timestamp = row[2]
    user_id = row[3]

    # Calculate the number of clicks per URL and country
    if url not in aggregated_data:
        aggregated_data[url] = {}

    if country not in aggregated_data[url]:
        aggregated_data[url][country] = {
            'clicks': 0,
            'unique_users': set(),
            'total_time': 0,
            'average_time': 0
        }

    aggregated_data[url][country]['clicks'] += 1

    # Calculate unique users per URL and country
    aggregated_data[url][country]['unique_users'].add(user_id)

    # Calculate total time and average time spent per URL and country
    previous_timestamp = aggregated_data[url][country]['total_time']
    time_difference = (timestamp - previous_timestamp).total_seconds() if previous_timestamp else 0
    aggregated_data[url][country]['total_time'] += time_difference

    # Calculate average time spent
    aggregated_data[url][country]['average_time'] = aggregated_data[url][country]['total_time'] / aggregated_data[url][country]['clicks']

# Print the aggregated data
for url, country_data in aggregated_data.items():
    for country, data in country_data.items():
        print(f"URL: {url}, Country: {country}")
        print(f"Number of Clicks: {data['clicks']}")
        print(f"Unique Users: {len(data['unique_users'])}")
        print(f"Average Time Spent: {data['average_time']} seconds")
        print()

from elasticsearch import Elasticsearch

# Configure Elasticsearch connection
hosts = ['localhost']  # Replace with the Elasticsearch host(s)
port = 9200  # Replace with the Elasticsearch port

# Create an Elasticsearch client
es = Elasticsearch(hosts=hosts, port=port)

# Define the index mapping
index_mapping = {
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "country": {"type": "keyword"},
            "clicks": {"type": "integer"},
            "unique_users": {"type": "integer"},
            "average_time": {"type": "float"}
        }
    }
}

# Create the index with the defined mapping
index_name = "processed_clickstream_data"  # Replace with your desired index name
es.indices.create(index=index_name, body=index_mapping)

# Iterate over the processed data and index it into Elasticsearch
for url, country_data in aggregated_data.items():
    for country, data in country_data.items():
        # Prepare the document to be indexed
        document = {
            "url": url,
            "country": country,
            "clicks": data["clicks"],
            "unique_users": len(data["unique_users"]),
            "average_time": data["average_time"]
        }

        # Index the document into Elasticsearch
        es.index(index=index_name, body=document)

# Example: Search for clickstream data by URL
search_query = {
    "query": {
        "term": {
            "url": "example.com"
        }
    }
}

# Perform the search query
search_results = es.search(index=index_name, body=search_query)

# Process and retrieve the search results
for hit in search_results["hits"]["hits"]:
    source = hit["_source"]
    url = source["url"]
    country = source["country"]
    clicks = source["clicks"]
    unique_users = source["unique_users"]
    average_time = source["average_time"]

    # Process or use the retrieved data as required
    print(f"URL: {url}")
    print(f"Country: {country}")
    print(f"Clicks: {clicks}")
    print(f"Unique Users: {unique_users}")
    print(f"Average Time: {average_time}")
    print()

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ClickstreamDataProcessing") \
    .getOrCreate()

# Read clickstream data from MySQL
clickstream_data = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/db_name") \
    .option("dbtable", "clickstream_data_table") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

from pyspark.sql.functions import avg, count, col

# Perform aggregations by URL and country
aggregated_data = clickstream_data.groupBy("url", "country") \
    .agg(
        count("url").alias("clicks"),
        countDistinct("user_id").alias("unique_users"),
        avg("time_spent").alias("average_time")
    )

# Write the processed data to Elasticsearch
aggregated_data.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "processed_clickstream_data") \
    .save()

$ spark-submit --class com.example.ClickstreamDataProcessor --master <spark_master_url> --deploy-mode <deploy_mode> clickstream_data_processor.jar


