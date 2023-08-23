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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="huytd18",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Join
Join_node1692799228024 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerTrustedZone_node1692799149863,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692799454592,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://huytd18-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
