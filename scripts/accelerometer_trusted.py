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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1701280899698 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://shengnan-zhao-lake-house/project/customer/trusted/run-1701281276835-part-r-00000"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1701280899698",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1701280843154 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://shengnan-zhao-lake-house/project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1701280843154",
)

# Script generated for node Join
Join_node1701281449171 = Join.apply(
    frame1=CustomerTrustedZone_node1701280899698,
    frame2=AccelerometerLanding_node1701280843154,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1701281449171",
)

# Script generated for node Drop Fields manually
SqlQuery777 = """
select distinct user, timestamp, x, y, z
from myDataSource
"""
DropFieldsmanually_node1701281650886 = sparkSqlQuery(
    glueContext,
    query=SqlQuery777,
    mapping={"myDataSource": Join_node1701281449171},
    transformation_ctx="DropFieldsmanually_node1701281650886",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1701281971298 = glueContext.getSink(
    path="s3://shengnan-zhao-lake-house/project/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1701281971298",
)
AccelerometerTrusted_node1701281971298.setCatalogInfo(
    catalogDatabase="nancy_zhao", catalogTableName="Accelerometer_Trusted"
)
AccelerometerTrusted_node1701281971298.setFormat("json")
AccelerometerTrusted_node1701281971298.writeFrame(DropFieldsmanually_node1701281650886)
job.commit()
