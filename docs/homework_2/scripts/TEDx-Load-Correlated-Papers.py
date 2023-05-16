###### TEDx-Load-Correlated-Papers #####

import sys
import json
import requests
import pyspark
from pyspark.sql.functions import *

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

# Read parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Start job context and job
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get collection
mongo_uri = "mongodb+srv://XXXX:XXXX@XXXX.mongodb.net"
print(mongo_uri)

mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2023",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}

# Create a dynamic frame from MongoDB
dynamic_frame = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options=mongo_options)

# Convert the dynamic frame to a DataFrame
data_frame = dynamic_frame.toDF()

# Iterate over the records in the DataFrame
for row in data_frame.head(1):
    url = "https://api.core.ac.uk/v3/search/journals/"
    
    search = []
    for tag in row.tags:
        search.append("(" + tag + ")")
    search = 'OR'.join(search)

    headers = {"Authorization": "Bearer XXXXX"}
    response = requests.get(url=url, params={"q": search}, headers=headers)
    
    # Process the response JSON
    df1 = sc.parallelize(response.json().get("results")).map(lambda x: json.dumps(x))
    df1 = spark.read.json(df1)
    df1 = df1.withColumn("idx", lit(row._id))
    
    df1.printSchema()
    
    # Aggregate the data by idx and collect a list of struct(title, identifiers) as 'papers'
    df1 = df1.groupBy(col("idx")).agg(collect_list(struct("title", "identifiers")).alias("papers"))
    df1.printSchema()
    
    # Join the processed DataFrame with the original DataFrame based on _id and drop 'idx'
    data_frame = data_frame.join(df1, data_frame._id == df1.idx, "left") \
    .drop("idx")
    
    data_frame.printSchema()

# Convert the DataFrame back to a dynamic frame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "nested")

# Write the dynamic frame to MongoDB
glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=mongo_options)
