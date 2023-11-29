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

# Script generated for node Customer Landing
CustomerLanding_node1701279566996 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://shengnan-zhao-lake-house/project/customer/landing/customer-1691348231425.json"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1701279566996",
)

# Script generated for node Customer Privacy Filter
SqlQuery857 = """
select * 
from myDataSource
where sharewithresearchasofdate is not null

"""
CustomerPrivacyFilter_node1701279632205 = sparkSqlQuery(
    glueContext,
    query=SqlQuery857,
    mapping={"myDataSource": CustomerLanding_node1701279566996},
    transformation_ctx="CustomerPrivacyFilter_node1701279632205",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1701279798624 = glueContext.getSink(
    path="s3://shengnan-zhao-lake-house/project/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1701279798624",
)
CustomerTrusted_node1701279798624.setCatalogInfo(
    catalogDatabase="nancy_zhao", catalogTableName="customer_trusted"
)
CustomerTrusted_node1701279798624.setFormat("json")
CustomerTrusted_node1701279798624.writeFrame(CustomerPrivacyFilter_node1701279632205)
job.commit()
