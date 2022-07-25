#praveen-changes

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Transformation").getOrCreate()
import json
import sys
from datetime import date
from pyspark.sql import functions as f
from pyspark.sql.functions import col, concat_ws, sha2
from pyspark.sql.types import DecimalType,StringType,DateType


config_path = sys.argv[1]
file_name = sys.argv[3]
lookup_result = sys.argv[4]

#config_path="s3://praveen-s3-landingzone/config_file/app_config.json"
config_data = spark.sparkContext.textFile(config_path).collect()
DataString = ''.join(config_data)
jsonData = json.loads(DataString)


landing_actives_path=jsonData["ingest-actives"]["source"]["file_location"]
landing_actives_format=jsonData["ingest-actives"]["source"]["file_format"]
landing_viewership_path=jsonData["ingest-Viewership"]["source"]["file_location"]
landing_viewership_format=jsonData["ingest-Viewership"]["source"]["file_format"]

raw_actives_path=jsonData["ingest-actives"]["destination"]["file_location"]
raw_actives_format=jsonData["ingest-actives"]["destination"]["file_format"]
raw_viewership_path=jsonData["ingest-Viewership"]["destination"]["file_location"]
raw_viewership_format=jsonData["ingest-Viewership"]["destination"]["file_format"]

def read_data(path,format):
    df=spark.read.parquet(path)
    return df

def write_data(data,path,format):
    data.write.mode("append").parquet(path)

actives_transform_columns=jsonData["processed-actives"]["transformation_columns"]
actives_masked_columns=jsonData["processed-actives"]["masked_columns"]

viewership_transform_columns=jsonData["processed-Vieweship"]["transformed_columns"]
viewership_masked_columns=jsonData["processed-Vieweship"]["masked_columns"]

#reusing the old variables:
#raw_actives_path,raw_actives_format,raw_viewership_path,raw_viewership_format






def casting(df,transform_columns):
    key_list=[]
    for key in transform_columns.keys():
        key_list.append(key)
    for columns in key_list:
        #print(transform_columns[columns].split(',')[0])
        if((transform_columns[columns].split(','))[0]=="DecimalType"):
            df=df.withColumn(columns,df[columns].cast(DecimalType(scale=int(transform_columns[columns].split(',')[1]))))
        elif(transform_columns[columns]=="StringType"):
            df = df.withColumn(columns,f.concat_ws(",",'city','state','location_category'))
    return df

def masking(df,columns):
    for column in columns:
        df = df.withColumn(column,sha2(col(column),256))
    return df


#file_name="viewership"
print(f"File Name = {file_name}")

if "actives" in file_name:
    raw_actives_data = read_data(raw_actives_path, raw_actives_format)
    transformed_actives=casting(raw_actives_data,actives_transform_columns)
    masked_actives = masking(transformed_actives, actives_masked_columns)
    masked_actives.write.partitionBy(jsonData["processed-actives"]["partition_columns"]).mode("overwrite").save(jsonData["processed-actives"]["destination"]["file_location"])
    print("Actives Data Processed")


elif "viewership" in file_name:
    raw_viewership_data = read_data(raw_viewership_path, raw_viewership_format)
    transformed_viewership=casting(raw_viewership_data,viewership_transform_columns)
    masked_viewership=masking(transformed_viewership,viewership_masked_columns)
    masked_viewership.write.partitionBy(jsonData["processed-Vieweship"]["partition_columns"]).mode("overwrite").save(jsonData["processed-Vieweship"]["destination"]["file_location"])
    print("Viewership Data Processed")


else:
    raw_actives_data = read_data(raw_actives_path, raw_actives_format)
    raw_viewership_data = read_data(raw_viewership_path, raw_viewership_format)
    transformed_actives = casting(raw_actives_data, actives_transform_columns)
    masked_actives = masking(transformed_actives, actives_masked_columns)
    masked_actives.write.partitionBy(jsonData["processed-actives"]["partition_columns"]).mode("overwrite").save(jsonData["processed-actives"]["destination"]["file_location"])
    print("Actives Data Processed")
    transformed_viewership = casting(raw_viewership_data, viewership_transform_columns)
    masked_viewership = masking(transformed_viewership, viewership_masked_columns)
    masked_viewership.write.partitionBy(jsonData["processed-Vieweship"]["partition_columns"]).mode("overwrite").save(jsonData["processed-Vieweship"]["destination"]["file_location"])
    print("Viewership Data Processed")

def delete_object(bucket_name, object_name):
    # Delete the object
    import boto3
    s3 = boto3.client('s3')
    try:
        s3.delete_object(Bucket=bucket_name, Key=object_name)
        s3.delete_object(Bucket="praveen-s3-stagingzone", Key="actives/")
    except ClientError as e:
        logging.error(e)
        return False
    return True

def lookup_dataset(jsonData,file_name,lookup_result):
    from pyspark.sql.types import StructType, StructField, StringType, DateType
    from pyspark.sql import functions as f
    from pyspark.sql.functions import isnull, when, lit, sha2, col
    if("actives" in file_name):
        if(int(lookup_result)):
            print("Started reading")
            staging_source_path = jsonData["lookup-Actives"]["staging_source"]["file_location"]
            raw_source_path = jsonData["lookup-Actives"]["raw_source"]["file_location"]
            dest_path = jsonData["lookup-Actives"]["staging_source"]["file_location"]
            temp_path = "s3://praveen-s3-stagingzone/Lookup_Dataset/temp/actives/"
            raw_masked_columns = ["advertising_id", "user_id"]
            staging_masked_columns = ["advertising_id", "user_id", "active_status", "begin_date", "update_date"]
            print("Datas read successfully")
            staging_source = spark.read.load(staging_source_path)
            staging_source = staging_source.select(staging_masked_columns)
            staging_source.show()

            raw_source = spark.read.load(raw_source_path)
            raw_source = raw_source.select(raw_masked_columns)
            raw_source = raw_source.withColumn("begin_date", f.current_date())
            raw_source = raw_source.withColumn("update_date", f.current_date())
            raw_source.show()

            staging_source_copy = staging_source

            datas = staging_source.join(raw_source, staging_source.advertising_id == raw_source.advertising_id,"outer").select(staging_source["*"],raw_source["advertising_id"].alias("raw_advertising_id"),raw_source["user_id"].alias("raw_user_id"),raw_source["begin_date"].alias("raw_begin_date"),raw_source["update_date"].alias("raw_update_date"))

            joined_datas = datas.where((datas.user_id != datas.raw_user_id) & (~isnull(datas.raw_user_id)) & (datas.active_status == 1) | isnull(datas.advertising_id)).collect()

            insertSchema = StructType([StructField("advertising_id", StringType(), True),StructField("user_id", StringType(), True),StructField("active_status", StringType(), True),StructField("begin_date", DateType(), True),StructField("update_date", DateType(), True)])

            for row in joined_datas:
                if (row[0] != None):  # row0 is staging_advertising_id
                    staging_source_copy = staging_source_copy.withColumn("active_status", when(staging_source_copy.advertising_id == row[0], 0).otherwise(staging_source_copy.active_status))
                    staging_source_copy = staging_source_copy.withColumn("update_date", when(staging_source_copy.advertising_id == row[0], f.current_date()).otherwise(staging_source_copy.update_date))
                    staging_source_copy = staging_source_copy.collect()

                    staging_source_copy.append({"advertising_id": row[5], "user_id": row[6], "active_status": 1, "begin_date": row[7],"update_date": row[8]})

                    staging_source_copy = spark.createDataFrame(staging_source_copy, insertSchema)
                    staging_source_copy.show()

                else:
                    staging_source_copy = staging_source_copy.collect()
                    staging_source_copy.append({"advertising_id": row[5], "user_id": row[6], "active_status": 1, "begin_date": row[7],"update_date": row[8]})
                    staging_source_copy = spark.createDataFrame(staging_source_copy, insertSchema)
                    staging_source_copy.show()

            staging_source_copy = staging_source_copy.select(staging_source_copy["advertising_id"],sha2(col("advertising_id"), 256).alias("masked_advertising_id"), staging_source_copy.user_id,sha2(col("user_id"), 256).alias("masked_user_id"),staging_source_copy.active_status,staging_source_copy.begin_date,staging_source_copy.update_date)
            print("reached till save")
            staging_source_copy.coalesce(1).write.mode("overwrite").save(temp_path)
            temp_data=spark.read.load(temp_path)
            temp_data.coalesce(1).write.mode("overwrite").save(dest_path)
            print("saved successfully")
        else:
            print("Started reading Data")
            raw_source_path = jsonData["lookup-Actives"]["raw_source"]["file_location"]
            dest_path = jsonData["lookup-Actives"]["staging_source"]["file_location"]
            raw_masked_columns = ["advertising_id", "user_id"]
            print("completed reading")
            raw_source = spark.read.load(raw_source_path)
            raw_source = raw_source.select(raw_masked_columns)
            raw_source = raw_source.withColumn("active_status", lit(1))
            raw_source.show()

            raw_source_copy = raw_source

            raw_source_copy = raw_source_copy.select(raw_source_copy.advertising_id,sha2(col("advertising_id"), 256).alias("masked_advertising_id"),raw_source_copy.user_id,sha2(col("user_id"), 256).alias("masked_user_id"),raw_source_copy.active_status)
            raw_source_copy = raw_source_copy.withColumn("begin_date", f.current_date())
            raw_source_copy = raw_source_copy.withColumn("update_date", f.current_date())

            raw_source_copy.coalesce(1).write.mode("overwrite").save(dest_path)
            print("Datas are saved successfully")
    else:
        if(int(lookup_result)):
            print("Started reading")
            staging_source_path = jsonData["lookup-Viewership"]["staging_source"]["file_location"]
            raw_source_path = jsonData["lookup-Viewership"]["raw_source"]["file_location"]
            dest_path = jsonData["lookup-Viewership"]["staging_source"]["file_location"]
            temp_path = "s3://praveen-s3-stagingzone/Lookup_Dataset/temp/viewership/"

            staging_masked_columns = ["advertising_id", "active_status", "begin_date", "update_date"]
            print("Datas read successfully")

            staging_data = spark.read.load(staging_source_path)

            staging_data = staging_data.select(staging_masked_columns)
            staging_data.show()
            raw_data = spark.read.load(raw_source_path)
            raw_data = raw_data.withColumn("begin_date", f.current_date())
            raw_data = raw_data.withColumn("update_date", f.current_date())
            raw_data = raw_data.select("advertising_id", "begin_date", "update_date")
            # print(raw)
            staging_data_copy = staging_data
            raw_data.show()

            joined_data = staging_data.join(raw_data, staging_data.advertising_id == raw_data.advertising_id, "outer").select(staging_data["*"], raw_data["advertising_id"].alias("raw_advertising_id"),raw_data["begin_date"].alias("raw_begin_date"), raw_data["update_date"].alias("raw_update_date"))

            joined_data.show()

            joined_data.where(isnull(joined_data.advertising_id)).show()
            shortlisted = joined_data.where(isnull(joined_data.advertising_id)).collect()
            print(shortlisted)
            staging_data_copy = staging_data_copy.collect()

            for data in shortlisted:
                staging_data_copy.append({"advertising_id": data[4], "active_status": 1, "begin_date": data[5], "update_date": data[6]})

            schema = StructType([StructField("advertising_id", StringType(), True), StructField("active_status", StringType(), True),StructField("begin_date", DateType(), True), StructField("update_date", DateType(), True)])
            staging_data_copy = spark.createDataFrame(staging_data_copy, schema)
            staging_data_copy = staging_data_copy.select(staging_data_copy["advertising_id"],sha2(col("advertising_id"), 256).alias("masked_advertising_id"),staging_data_copy["active_status"],staging_data_copy["begin_date"],staging_data_copy["update_date"])
            staging_data_copy.show()
            print("reached till save")
            staging_data_copy.coalesce(1).write.mode("overwrite").save(temp_path)
            temp_data = spark.read.load(temp_path)
            temp_data.coalesce(1).write.mode("overwrite").save(dest_path)
            print("saved successfully")
        else:
            print("Reading data started")
            staging_source_path = jsonData["lookup-Viewership"]["staging_source"]["file_location"]
            raw_source_path = jsonData["lookup-Viewership"]["raw_source"]["file_location"]

            dest_path = staging_source_path

            print("Reading data completed")

            raw_masked_columns = ["advertising_id"]
            raw_source = spark.read.load(raw_source_path)
            raw_source = raw_source.select(raw_masked_columns)
            raw_source = raw_source.withColumn("active_status", lit(1))
            # raw_source.show()
            raw_source_copy = raw_source
            raw_source_copy = raw_source_copy.select(raw_source_copy.advertising_id,sha2(col("advertising_id"), 256).alias("masked_advertising_id"),raw_source_copy.active_status)
            raw_source_copy = raw_source_copy.withColumn("begin_date", f.current_date())
            raw_source_copy = raw_source_copy.withColumn("update_date", f.current_date())
            raw_source_copy.show()
            raw_source_copy.coalesce(1).write.mode("overwrite").save(dest_path)
            print("Datas are saved successfully")

lookup_dataset(jsonData,file_name,lookup_result)
spark.stop()
