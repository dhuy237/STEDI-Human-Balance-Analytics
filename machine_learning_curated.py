import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Step Trainer Curated
StepTrainerCurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="huytd18",
    table_name="step_trainer_curated",
    transformation_ctx="StepTrainerCurated_node1",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1692805040110 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="huytd18",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1692805040110",
    )
)

# Script generated for node Join
Join_node1692799228024 = Join.apply(
    frame1=StepTrainerCurated_node1,
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
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1692799454592",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1692805087237 = DynamicFrame.fromDF(
    DropFields_node1692799454592.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1692805087237",
)

# Script generated for node Join
Join_node1692805104246 = Join.apply(
    frame1=AccelerometerTrustedZone_node1692805040110,
    frame2=DropDuplicates_node1692805087237,
    keys1=["timestamp", "user"],
    keys2=["sensorreadingtime", "email"],
    transformation_ctx="Join_node1692805104246",
)

# Script generated for node Drop Fields
DropFields_node1692805314884 = DropFields.apply(
    frame=Join_node1692805104246,
    paths=["user", "serialnumber", "email"],
    transformation_ctx="DropFields_node1692805314884",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692805314884,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://huytd18-lake-house/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
