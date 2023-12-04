import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1701525370356 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1701525370356",
)

# Script generated for node Join
Join_node1701525389606 = Join.apply(
    frame1=accelerometer_trusted_node1701525370356,
    frame2=customertrusted_node1701522551985,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1701525389606",
)

# Script generated for node SQL Query
SqlQuery1647 = """
select customername,
email,
phone,
birthday,
serialnumber,
registrationdate,
sharewithresearchasofdate,
sharewithfriendsasofdate,
sharewithpublicasofdate,
lastupdatedate
from myDataSource
"""
SQLQuery_node1701525439772 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1647,
    mapping={"myDataSource": Join_node1701525389606},
    transformation_ctx="SQLQuery_node1701525439772",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701526068245 = DynamicFrame.fromDF(
    SQLQuery_node1701525439772.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701526068245",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701525734974 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropDuplicates_node1701526068245,
    database="stedi-project",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1701525734974",
)

job.commit()
