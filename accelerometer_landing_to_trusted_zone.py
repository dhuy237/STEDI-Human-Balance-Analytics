import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="huytd18",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1692799149863 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://huytd18-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1692799149863",
)

# Script generated for node Join
Join_node1692799228024 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrustedZone_node1692799149863,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1692799228024",
)

# Script generated for node Drop Fields
DropFields_node1692799454592 = DropFields.apply(
    frame=Join_node1692799228024,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "customerName",
        "shareWithResearchAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1692799454592",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692799454592,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://huytd18-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
