#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDStoHDFS_Trans_Table").master("local").enableHiveSupport().getOrCreate()

sub_path="s3://finalprojectb9/data/subscriber/"
add_path="s3://finalprojectb9/data/address/"
cmp_path="s3://finalprojectb9/data/complaint/"
staff_path="s3://finalprojectb9/data/staff/"


#Storing data into Hbase
df_sb=spark.read.format("csv").option("header","true").option("inferschema","true").load(sub_path)
#df_sb.write.format("org.apache.phoenix.spark").option("table","SUBSCRIBER_HB").option("zkUrl","localhost:2181").mode('overwrite').save()


df_add=spark.read.format("csv").option("header","true").option("inferschema","true").load(add_path)
#df_add.write.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").mode('overwrite').save()


df_cmp=spark.read.format("csv").option("header","true").option("inferschema","true").load(cmp_path)
#df_cmp.write.format("org.apache.phoenix.spark").option("table","COMPLAINT_HB").option("zkUrl","localhost:2181").mode('overwrite').save()


df_stf=spark.read.format("csv").option("header","true").option("inferschema","true").load(staff_path)
#df_stf.write.format("org.apache.phoenix.spark").option("table","STAFF_HB").option("zkUrl","localhost:2181").mode('overwrite').save()

df_sb.show()
df_add.show()
df_cmp.show()
df_stf.show()


print("Data Successfully Stored in Hbase")

df_ct=spark.read.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl","localhost:2181").load()
df_cn=spark.read.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl","localhost:2181").load()
df_ppr=spark.read.format("org.apache.phoenix.spark").option("table","PLAN_PREPAID_HB").option("zkUrl","localhost:2181").load()
df_ppo=spark.read.format("org.apache.phoenix.spark").option("table","PLAN_POSTPAID_HB").option("zkUrl","localhost:2181").load()


df_sb.createOrReplaceTempView("subscriber")
df_add.createOrReplaceTempView("address")
df_cmp.createOrReplaceTempView("COMPLAINT")
df_stf.createOrReplaceTempView("Staff")

df_ct.createOrReplaceTempView("city")
df_cn.createOrReplaceTempView("country")
df_ppr.createOrReplaceTempView("plan_prepaid")
df_ppo.createOrReplaceTempView("plan_postpaid")


print("Processing Context for Subscriber Details")

df_result_sd=spark.sql("""SELECT
				S.sid as subscriberId,
				S.name as subscribername,
				S.mob as contactnumber,
				S.email as emailId,
				Ad.street as address,
				ct.ct_name as city,
				cn.cn_name as country,
				S.sys_cre_date as create_date,
				S.sys_upd_date as update_date,
				S.Active_flag as Active_flag,
				ppr.plan_desc as prepaid_desc,
				ppr.amount as pre_amount,
				ppo.plan_desc as postpaid_desc,
				ppo.amount as pos_amount
				from Subscriber S
				LEFT JOIN Address Ad on S.add_id=Ad.add_id
				Left Join City ct on Ad.ct_id=ct.ct_id
				Left join Country cn on ct.cn_id=cn.cn_id
				Left join Plan_Prepaid ppr on S.prepaid_plan_id=ppr.plan_id
				Left join Plan_Postpaid ppo on S.postpaid_plan_id=ppo.plan_id
""")

df_result_sd.show(10)

df_result_sd.write.mode('overwrite').saveAsTable("prod.subscriber_details_staging")

spark.stop()