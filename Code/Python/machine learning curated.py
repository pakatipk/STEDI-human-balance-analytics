import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1701527801057 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1701527801057",
)

# Script generated for node steptrainer_trusted
steptrainer_trusted_node1701527812939 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="steptrainer_trusted",
    transformation_ctx="steptrainer_trusted_node1701527812939",
)

# Script generated for node Join
Join_node1701527858174 = Join.apply(
    frame1=steptrainer_trusted_node1701527812939,
    frame2=accelerometer_trusted_node1701527801057,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1701527858174",
)

# Script generated for node SQL Query
SqlQuery1746 = """
select user, 
timestamp, 
serialnumber,
distancefromobject,
x, y, z from myDataSource

"""
SQLQuery_node1701527925193 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1746,
    mapping={"myDataSource": Join_node1701527858174},
    transformation_ctx="SQLQuery_node1701527925193",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701528291200 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1701527925193,
    database="stedi-project",
    table_name="machine_learning_curated",
    transformation_ctx="AWSGlueDataCatalog_node1701528291200",
)

job.commit()
