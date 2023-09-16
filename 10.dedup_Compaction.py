#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import desc


spark = SparkSession.builder.appName("Tel_dedup_Compaction").master("yarn").enableHiveSupport().getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

#################################Dedup Subcriber details######################

print("Subcriber details de-duplication and compaction started")
sd_dff=spark.sql("select * from prod.subscriber_details")
sd_dfs=spark.sql("select * from prod.subscriber_details_staging")

un = sd_dff.union(sd_dfs)

res=un.withColumn("new_upd",F.when(un.update_date.isNull(),F.to_timestamp(F.lit("1970-01-01 00:00:00 "),format="yyyy-MM-dd HH:mm:SS")).otherwise(un.update_date)).drop("update_date")


f=res.select("*").withColumnRenamed("new_upd","update_date")

res1=f.withColumn("rn", F.row_number().over(Window.partitionBy("subscriberId").orderBy(desc("update_date"))))

res2=res1.filter(res1.rn == 1).drop("rn")

res2.show(30)

res2.write.mode('overwrite').saveAsTable("prod.subscriber_details_temp")
spark.sql("truncate table prod.subscriber_details")
spark.sql("insert overwrite table prod.subscriber_details select subscriberid,subscribername,contactnumber,emailid,address,city,country,create_date,update_date,active_flag,prepaid_desc,pre_amount,postpaid_desc,pos_amount from prod.subscriber_details_temp")

spark.sql("truncate table prod.subscriber_details_staging")
spark.sql("drop table prod.subscriber_details_temp")

print("subscriber details de-duplication finished successfully and data pushed into final table")

###################################Dedup Sub details completed##############

#################################Dedup complaint_details######################

print("Suncriber details de-duplication and compaction started")
cd_dff=spark.sql("select * from prod.complaint_details")
cd_dfs=spark.sql("select * from prod.complaint_details_staging")

un = cd_dff.union(cd_dfs)

res=un.withColumn("new_upd",F.when(un.com_upd_date.isNull(),F.to_timestamp(F.lit("1970-01-01 00:00:00 "),format="yyyy-MM-dd HH:mm:SS")).otherwise(un.com_upd_date)).drop("com_upd_date")


f=res.select("*").withColumnRenamed("new_upd","com_upd_date")

res1=f.withColumn("rn", F.row_number().over(Window.partitionBy("complaintId").orderBy(desc("com_upd_date"))))

res2=res1.filter(res1.rn == 1).drop("rn")

res2.show(30)

res2.write.mode('overwrite').saveAsTable("prod.complaint_details_temp")
spark.sql("truncate table prod.complaint_details")
spark.sql("insert overwrite table prod.complaint_details select subscriberId,subscribername,complaintId,complaintReg,description,com_cre_date,com_upd_date,status from prod.complaint_details_temp")

spark.sql("truncate table prod.complaint_details_staging")
spark.sql("drop table prod.complaint_details_temp")

print("complaint details de-duplication finished successfully and data pushed into final table")

###################################Dedup complaint details completed##############

spark.stop()
