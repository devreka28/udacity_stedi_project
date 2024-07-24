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

# Script generated for node Customer Curated
CustomerCurated_node1721829691944 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacitycoursero3/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1721829691944")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1721829758262 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacitycoursero3/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1721829758262")

# Script generated for node Renamed keys for Join Customer Curated and Step Trainer Landing
RenamedkeysforJoinCustomerCuratedandStepTrainerLanding_node1721843484139 = ApplyMapping.apply(frame=CustomerCurated_node1721829691944, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("sharewithpublicasofdate", "bigint", "right_sharewithpublicasofdate", "bigint"), ("birthday", "string", "right_birthday", "string"), ("registrationdate", "bigint", "right_registrationdate", "bigint"), ("sharewithresearchasofdate", "bigint", "right_sharewithresearchasofdate", "bigint"), ("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("lastupdatedate", "bigint", "right_lastupdatedate", "bigint"), ("phone", "string", "right_phone", "string"), ("sharewithfriendsasofdate", "bigint", "right_sharewithfriendsasofdate", "bigint")], transformation_ctx="RenamedkeysforJoinCustomerCuratedandStepTrainerLanding_node1721843484139")

# Script generated for node Change Schema
ChangeSchema_node1721843548686 = ApplyMapping.apply(frame=StepTrainerLanding_node1721829758262, mappings=[("sensorreadingtime", "bigint", "sensorreadingtime", "long"), ("serialnumber", "string", "serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1721843548686")

# Script generated for node Join Customer Curated and Step Trainer Landing
JoinCustomerCuratedandStepTrainerLanding_node1721829815125 = Join.apply(frame1=RenamedkeysforJoinCustomerCuratedandStepTrainerLanding_node1721843484139, frame2=ChangeSchema_node1721843548686, keys1=["right_serialnumber"], keys2=["serialnumber"], transformation_ctx="JoinCustomerCuratedandStepTrainerLanding_node1721829815125")

# Script generated for node All step trainer records for curated customers
SqlQuery0 = '''
select distinct sensorreadingtime, serialnumber, distancefromobject from myDataSource
'''
Allsteptrainerrecordsforcuratedcustomers_node1721830212240 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinCustomerCuratedandStepTrainerLanding_node1721829815125}, transformation_ctx = "Allsteptrainerrecordsforcuratedcustomers_node1721830212240")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1721830279793 = glueContext.getSink(path="s3://udacitycoursero3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1721830279793")
StepTrainerTrusted_node1721830279793.setCatalogInfo(catalogDatabase="udacity_stedi_project",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1721830279793.setFormat("json")
StepTrainerTrusted_node1721830279793.writeFrame(Allsteptrainerrecordsforcuratedcustomers_node1721830212240)
job.commit()
