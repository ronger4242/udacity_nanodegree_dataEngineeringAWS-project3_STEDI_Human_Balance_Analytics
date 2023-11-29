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

# Script generated for node step trainer landing
steptrainerlanding_node1701294720144 = glueContext.create_dynamic_frame.from_catalog(
    database="nancy_zhao",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1701294720144",
)

# Script generated for node customer curated
customercurated_node1701294735073 = glueContext.create_dynamic_frame.from_catalog(
    database="nancy_zhao",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1701294735073",
)

# Script generated for node SQL Query
SqlQuery942 = """
select s.*
from s
join c
on s.serialnumber = c.serialnumber
"""
SQLQuery_node1701294763642 = sparkSqlQuery(
    glueContext,
    query=SqlQuery942,
    mapping={
        "s": steptrainerlanding_node1701294720144,
        "c": customercurated_node1701294735073,
    },
    transformation_ctx="SQLQuery_node1701294763642",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1701295004543 = glueContext.getSink(
    path="s3://shengnan-zhao-lake-house/project/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1701295004543",
)
step_trainer_trusted_node1701295004543.setCatalogInfo(
    catalogDatabase="nancy_zhao", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1701295004543.setFormat("json")
step_trainer_trusted_node1701295004543.writeFrame(SQLQuery_node1701294763642)
job.commit()
