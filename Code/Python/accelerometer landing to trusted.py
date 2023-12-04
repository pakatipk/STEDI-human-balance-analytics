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

# Script generated for node customer trusted
customertrusted_node1701522551985 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_landing",
    transformation_ctx="customertrusted_node1701522551985",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1701522562951 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1701522562951",
)

# Script generated for node Join
Join_node1701522614787 = Join.apply(
    frame1=accelerometerlanding_node1701522562951,
    frame2=customertrusted_node1701522551985,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1701522614787",
)

# Script generated for node SQL Query
SqlQuery1670 = """
select user, timestamp, x, y, z from myDataSource;

"""
SQLQuery_node1701524008992 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1670,
    mapping={"myDataSource": Join_node1701522614787},
    transformation_ctx="SQLQuery_node1701524008992",
)

# Script generated for node accelerrometer trusted
accelerrometertrusted_node1701522637742 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1701524008992,
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerrometertrusted_node1701522637742",
)

job.commit()
