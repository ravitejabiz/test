import sys
#from IPython.display import display
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
import boto3
import datetime

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

bucket_name = "dw-etl-cw-dev"
s3_file_name_main = "Process/AP/AP/interface_ap.dat"

# node S3 bucket
# S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
#     database="dw-cw-source-s3-prod",
#     table_name="interface_ap_dat",
#     transformation_ctx="S3bucket_node1",
# )


# Filter data for CW
# Filter_node = Filter.apply(
#     frame=S3bucket_node1,
#     f=lambda row: (bool(re.match("CW", row["col0"]))),
#     transformation_ctx="Filter_node",
# )

Filter_node = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f's3://{bucket_name}/{s3_file_name_main}']},
    format="csv",
    format_options={
        "withHeader": False,
        "separator": "|",
        "optimizePerformance": True,
        "encoding": "windows-1252"
    },
)
Filter_node.printSchema()
#print(Filter_node)
dataframe = Filter_node.toDF()
#filtered_dataframe = dataframe.filter(dataframe["col1"] == "4FDA688C-DF03-4339-B545-F75D932AE755")
#filtered_dataframe.show(truncate=False)
pattern = r'[^\w\s@\/|()`~*$#@!-,.;:\[{("]'
columns_to_process = [col_name for col_name in dataframe.columns]
#processed_df = dataframe
for col_name in columns_to_process:
    processed_df = dataframe.withColumn(col_name, regexp_replace(col(col_name), pattern, "-"))
processed_df.show(truncate=False)

# dyf = DynamicFrame.fromDF(processed_df, glueContext, "dyf")

# #ApplyMapping
# ApplyMapping_node2 = ApplyMapping.apply(
#     frame=dyf,
#     mappings=[
#         ("col0", "string", "source_system", "string"),
#         ("col1", "string", "id", "string"),
#         ("col2", "string", "file_nbr", "string"),
#         ("col3", "string", "vendor_id", "string"),
#         ("col4", "long", "subsidiary", "int"),
#         ("col5", "string", "invoice_nbr", "string"),
#         ("col6", "string", "invoice_date", "timestamp"),
#         ("col7", "string", "period", "string"),
#         ("col8", "string", "ref_nbr", "string"),
#         ("col9", "string", "housebill_nbr", "string"),
#         ("col10", "string", "master_bill_nbr", "string"),
#         ("col11", "string", "consol_nbr", "string"),
#         ("col12", "string", "business_segment", "string"),
#         ("col13", "string", "invoice_type", "string"),
# 		("col14", "string", "handling_stn", "string"),
#         ("col15", "string", "controlling_stn", "string"),
#         ("col16", "string", "charge_cd", "string"),
#         ("col17", "string", "charge_cd_desc", "string"),
#         ("col18", "string", "charge_cd_internal_id", "bigint"),
#         ("col19", "string", "currency", "string"),
#         ("col20", "double", "rate", "decimal"),
#         ("col21", "double", "total", "decimal"),
#         ("col22", "string", "sales_person", "string"),
#         ("col23", "string", "email", "string"),
#         ("col24", "string", "posted_date", "timestamp"),
#         ("col25", "string", "finalizedby", "string"),
#         ("col26", "string", "processed", "string"),
#         ("col27", "string", "processed_date", "date"),
#         ("col28", "string", "vendor_internal_id", "string"),
#         ("col29", "string", "currency_internal_id", "string"),
#         ("col30", "string", "internal_id", "bigint"),
#         ("col31", "string", "load_create_date", "timestamp"),
#         ("col32", "string", "load_update_date", "timestamp"),
#         ("col33", "string", "invoice_vendor_num", "string"),
#         ("col34", "string", "finalized_date", "timestamp"),
#         ("col35", "string", "bill_to_nbr", "string"),
#         ("col36", "string", "line_type", "string"),
#         ("col37", "string", "gc_code", "string"),
#         ("col38", "string", "gc_name", "string"),
#         ("col39", "string", "tax_code", "string"),
#         ("col40", "string", "tax_code_internal_id", "string"),
#         ("col41", "string", "intercompany", "string"),
#         ("col42", "string", "intercompany_processed", "string"),
#         ("col43", "string", "intercompany_processed_date", "date"),
#         ("col44", "string", "pairing_available_flag", "string"),
#         ("col45", "string", "internal_ref_nbr", "string"),
#         ("col46", "string", "mode_name", "string"),
#         ("col47", "string", "service_level", "string"),
#         ("col48", "string", "unique_ref_nbr", "string"),
#     ],
#     transformation_ctx="ApplyMapping_node2",
# )


# # S3 bucket
# ##S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
# ##    frame=ApplyMapping_node2,
# ##    connection_type="s3",
# ##    format="csv",
# ##    connection_options={"path": "s3://stage-cw-test3-tables/AP/", "partitionKeys": []},
# ##    transformation_ctx="S3bucket_node3",
# ##)

# # Redshift Copy

# pre_query="begin;truncate table public.interface_ap_cw_stage;insert into interface_ap_his (select * from interface_ap_cw a where (a.invoice_nbr,a.invoice_type ,a.file_nbr,a.vendor_id) in (select invoice_nbr,invoice_type,file_nbr,vendor_id from interface_ap_master_cw where processed='P' and source_system='CW' and (intercompany='N' or (intercompany='Y' and intercompany_processed='P'))) and source_system='CW');insert into interface_ap_master_his (select * from interface_ap_master_cw where processed='P' and source_system='CW' and (intercompany='N' or (intercompany='Y' and intercompany_processed='P')));update interface_ap_his set internal_id =c.internal_id,processed=c.processed,intercompany_processed=c.intercompany_processed from interface_ap_master_cw c where c.invoice_nbr=interface_ap_his.invoice_nbr and c.invoice_type=interface_ap_his.invoice_type and c.file_nbr=interface_ap_his.file_nbr and c.vendor_id =interface_ap_his.vendor_id and c.source_system='CW';delete from interface_ap_cw where (invoice_nbr,invoice_type,file_nbr,vendor_id) in (select invoice_nbr,invoice_type,file_nbr,vendor_id from interface_ap_master_cw where processed='P' and source_system='CW' and (intercompany='N' or (intercompany='Y' and intercompany_processed='P'))) and source_system='CW';delete from interface_ap_master_cw where processed='P' and source_system='CW' and (intercompany='N' or (intercompany='Y' and intercompany_processed='P'));end;"

# post_query="begin;update interface_ap_cw set vendor_id =c.vendor_id,invoice_nbr=c.invoice_nbr from interface_ap_cw_stage c where c.id=interface_ap_cw.id and interface_ap_cw.source_system='CW' and interface_ap_cw.vendor_id is null;delete from interface_ap_cw_stage using interface_ap_his t where t.id = interface_ap_cw_stage.id and t.source_system='CW';delete from interface_ap_cw_stage using interface_ap_cw t where t.id = interface_ap_cw_stage.id and t.source_system='CW';update interface_ap_cw_stage set sales_person =null where sales_person ='NULL';update interface_ar_cw_stage set business_segment =null where business_segment ='NULL';update interface_ap_cw_stage set email =null where email ='NULL';update interface_ap_cw_stage set mode_name =null where mode_name ='NULL';update interface_ap_cw_stage set service_level =null where service_level ='NULL';update interface_ap_cw_stage set finalizedby =null where finalizedby ='NULL';update interface_ap_cw_stage set tax_code =null where tax_code ='NULL';update interface_ap_cw_stage set tax_code_internal_id =c.internal_id from netsuit_taxcodes_cw c where c.company =interface_ap_cw_stage.gc_code and c.tax_code=interface_ap_cw_stage.tax_code;update interface_ap_cw_stage set currency_internal_id =c.currency_internal_id from netsuite_currency c where c.curr_symbol =interface_ap_cw_stage.currency ;update interface_ap_cw_stage set charge_cd_internal_id =c.internal_id from xref_charge_code c where c.charge_cd =interface_ap_cw_stage.charge_cd and c.description =interface_ap_cw_stage.charge_cd_desc ;update interface_ap_cw_stage set charge_cd_internal_id =c.internal_id from xref_charge_code c where c.charge_cd =interface_ap_cw_stage.charge_cd and  interface_ap_cw_stage.charge_cd_internal_id is null;update interface_ap_cw_stage set vendor_internal_id =c.vendor_internal_id ,subsidiary =c.subsidiary_internal_id from netsuit_vendors_cw c where c.company =interface_ap_cw_stage.gc_code and interface_ap_cw_stage.vendor_id=c.vendor_id and interface_ap_cw_stage.intercompany = 'Y' and interface_ap_cw_stage.gc_code !='OTS';insert into public.interface_ap_cw select * from public.interface_ap_cw_stage ;insert into public.interface_ap_master_cw select distinct source_system ,invoice_nbr ,invoice_type, vendor_id, vendor_internal_id , currency_internal_id ,internal_id ,processed ,processed_date , invoice_vendor_num ,intercompany, intercompany_processed,intercompany_processed_date, unique_ref_nbr,file_nbr,gc_code,pairing_available_flag from public.interface_ap_cw_stage;update public.interface_ap_cw set pairing_available_flag ='Y' where intercompany ='Y' and source_system ='CW' and (file_nbr,invoice_type,unique_ref_nbr) in (select distinct ar.file_nbr , ar.invoice_type,ar.unique_ref_nbr from (select distinct file_nbr,invoice_type ,unique_ref_nbr from interface_ar_cw ia where intercompany = 'Y' and source_system='CW')ar join  (select distinct file_nbr  ,invoice_type ,unique_ref_nbr ,total from interface_ap_cw where intercompany = 'Y' and source_system='CW')ap on ar.file_nbr = ap.file_nbr and ar.invoice_type = ap.invoice_type and ar.unique_ref_nbr = ap.unique_ref_nbr);update public.interface_ar_cw set pairing_available_flag ='Y' where intercompany ='Y' and source_system ='CW' and (file_nbr,invoice_type,unique_ref_nbr) in (select distinct ar.file_nbr , ar.invoice_type,ar.unique_ref_nbr from (select distinct file_nbr,invoice_type ,unique_ref_nbr from interface_ar_cw ia where intercompany = 'Y' and source_system='CW')ar join  (select distinct file_nbr  ,invoice_type ,unique_ref_nbr ,total from interface_ap_cw where intercompany = 'Y' and source_system='CW')ap on ar.file_nbr = ap.file_nbr and ar.invoice_type = ap.invoice_type and ar.unique_ref_nbr = ap.unique_ref_nbr);update public.interface_ap_master_cw set pairing_available_flag ='Y' where intercompany ='Y' and source_system ='CW' and (file_nbr,invoice_type,unique_ref_nbr) in (select distinct ar.file_nbr , ar.invoice_type,ar.unique_ref_nbr from (select distinct file_nbr,invoice_type ,unique_ref_nbr from interface_ar_cw ia where intercompany = 'Y' and source_system='CW')ar join  (select distinct file_nbr  ,invoice_type ,unique_ref_nbr ,total from interface_ap_cw where intercompany = 'Y' and source_system='CW')ap on ar.file_nbr = ap.file_nbr and ar.invoice_type = ap.invoice_type and ar.unique_ref_nbr = ap.unique_ref_nbr);end;"

# DataSink0 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = ApplyMapping_node2, catalog_connection = "test_datamodel", connection_options = {"preactions":pre_query,"dbtable": "public.interface_ap_cw_stage", "database": "test_datamodel","postactions":post_query},redshift_tmp_dir = args["TempDir"], transformation_ctx = "DataSink0")


## Archiving

now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day
hour = now.hour
minute = now.minute
second = now.second

destfinal = (
            "Archive/AP/interface_ap-"
            + "{:0>4}".format(str(year))
            + "{:0>2}".format(str(month))
            + "{:0>2}".format(str(day))
            + "{:0>2}".format(str(hour)) 
            + "{:0>2}".format(str(minute))
            + "{:0>2}".format(str(second))
            + ".dat"
        )

copy_source = {
'Bucket': bucket_name,
'Key': s3_file_name_main
}

s3.meta.client.copy(copy_source, bucket_name, destfinal)

job.commit()