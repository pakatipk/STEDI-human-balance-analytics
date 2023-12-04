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

# Script generated for node customer_curated
customer_curated_node1701526839500 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1701526839500",
)

# Script generated for node steptrainer_landing
steptrainer_landing_node1701526826613 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="steptrainer_landing",
    transformation_ctx="steptrainer_landing_node1701526826613",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1701526925145 = ApplyMapping.apply(
    frame=steptrainer_landing_node1701526826613,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "s_serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1701526925145",
)

# Script generated for node Join
Join_node1701526861331 = Join.apply(
    frame1=customer_curated_node1701526839500,
    frame2=RenamedkeysforJoin_node1701526925145,
    keys1=["serialnumber"],
    keys2=["s_serialnumber"],
    transformation_ctx="Join_node1701526861331",
)

# Script generated for node SQL Query
SqlQuery1846 = """
select sensorreadingtime,
serialnumber,
distancefromobject from myDataSource

"""
SQLQuery_node1701526975280 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1846,
    mapping={"myDataSource": Join_node1701526861331},
    transformation_ctx="SQLQuery_node1701526975280",
)

# Script generated for node steptrainer_trusted
steptrainer_trusted_node1701527222980 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1701526975280,
    database="stedi-project",
    table_name="steptrainer_trusted",
    transformation_ctx="steptrainer_trusted_node1701527222980",
)

job.commit()
