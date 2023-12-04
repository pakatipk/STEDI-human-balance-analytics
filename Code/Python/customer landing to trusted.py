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

# Script generated for node Amazon S3
AmazonS3_node1701520900745 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-120123/customer/landing/data/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701520900745",
)

# Script generated for node SQL Query
SqlQuery1883 = """
select * from myDataSource
where sharewithresearchasofdate is not null
"""
SQLQuery_node1701520987980 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1883,
    mapping={"myDataSource": AmazonS3_node1701520900745},
    transformation_ctx="SQLQuery_node1701520987980",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701521271140 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1701520987980,
    database="stedi-project",
    table_name="customer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1701521271140",
)

job.commit()
