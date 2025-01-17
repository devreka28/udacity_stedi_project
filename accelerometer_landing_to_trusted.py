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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1721824944773 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacitycoursero3/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1721824944773")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1721825048304 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacitycoursero3/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1721825048304")

# Script generated for node Join customer trusted and accelerometer landing
Joincustomertrustedandaccelerometerlanding_node1721825114474 = Join.apply(frame1=AccelerometerLanding_node1721825048304, frame2=CustomerTrusted_node1721824944773, keys1=["user"], keys2=["email"], transformation_ctx="Joincustomertrustedandaccelerometerlanding_node1721825114474")

# Script generated for node SQL Query
SqlQuery0 = '''
select timeStamp, user, x, y, z from myDataSource
'''
SQLQuery_node1721825188635 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Joincustomertrustedandaccelerometerlanding_node1721825114474}, transformation_ctx = "SQLQuery_node1721825188635")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721825294358 = glueContext.getSink(path="s3://udacitycoursero3/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1721825294358")
AccelerometerTrusted_node1721825294358.setCatalogInfo(catalogDatabase="udacity_stedi_project",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1721825294358.setFormat("json")
AccelerometerTrusted_node1721825294358.writeFrame(SQLQuery_node1721825188635)
job.commit()
