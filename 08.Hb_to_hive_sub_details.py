#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hbtohive_sub_details").master("yarn").enableHiveSupport().getOrCreate()

#subscriber details
#sid,sname,mob,email,address,prepaid plan,postpaid plan , active
#subscriber,address,prepaid plan, postpaid plan , city , country

print("Subscriber details historical data computation started")

df_sb=spark.read.format("org.apache.phoenix.spark").option("table","SUBSCRIBER_HB").option("zkUrl","localhost:2181").load()
df_ad=spark.read.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").load()
df_ct=spark.read.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl","localhost:2181").load()
df_cn=spark.read.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl","localhost:2181").load()
df_ppr=spark.read.format("org.apache.phoenix.spark").option("table","PLAN_PREPAID_HB").option("zkUrl","localhost:2181").load()
df_ppo=spark.read.format("org.apache.phoenix.spark").option("table","PLAN_POSTPAID_HB").option("zkUrl","localhost:2181").load()

df_sb.createOrReplaceTempView("subscriber_tmp")
df_ad.createOrReplaceTempView("address_tmp")
df_ct.createOrReplaceTempView("city_tmp")
df_cn.createOrReplaceTempView("country_tmp")
df_ppr.createOrReplaceTempView("plan_prepaid_tmp")
df_ppo.createOrReplaceTempView("plan_postpaid_tmp")

df_result=spark.sql("""SELECT
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
				from subscriber_tmp S
				LEFT JOIN Address_tmp Ad on S.add_id=Ad.add_id
				Left Join City_tmp ct on Ad.ct_id=ct.ct_id
				Left join Country_tmp cn on ct.cn_id=cn.cn_id
				Left join Plan_Prepaid_tmp ppr on S.prepaid_plan_id=ppr.plan_id
				Left join Plan_Postpaid_tmp ppo on S.postpaid_plan_id=ppo.plan_id
""")

#df_result.show(10)
#df_result.write.mode('overwrite').saveAsTable("prod.subscriber_details_staging")


spark.catalog.dropTempView("subscriber_tmp")
spark.catalog.dropTempView("address_tmp")
spark.catalog.dropTempView("city_tmp")
spark.catalog.dropTempView("country_tmp")
spark.catalog.dropTempView("plan_prepaid_tmp")
spark.catalog.dropTempView("plan_postpaid_tmp")


#DSL
test=df_sb.join(df_ad,"add_id", how="left").join(df_ct,"ct_id", how="left").join(df_cn, "cn_id", how="left")
test1=test.join(df_ppr, df_ppr.PLAN_ID == test.PREPAID_PLAN_ID, how= "left").drop("ADD_ID").drop("CT_ID").drop("CN_ID").drop("PLAN_ID").withColumnRenamed("PLAN_DESC","PRE_PLAN_DESC").withColumnRenamed("AMOUNT","PRE_AMOUNT")

test2=test1.join(df_ppo, df_ppo.PLAN_ID == test1.POSTPAID_PLAN_ID, how= "left").drop("PLAN_ID").withColumnRenamed("PLAN_DESC","POS_PLAN_DESC").withColumnRenamed("AMOUNT","POS_AMOUNT")

#Give alias

res=test2.selectExpr("SID as subscriberId", "NAME as subscribername","MOB as contactnumber","EMAIL as emailId","STREET as address","CT_NAME as city","CN_NAME as country","SYS_CRE_DATE as create_date","SYS_UPD_DATE as update_date","ACTIVE_FLAG as Active_flag","PRE_PLAN_DESC as prepaid_desc","PRE_AMOUNT as pre_amount","POS_PLAN_DESC as postpaid_desc","POS_AMOUNT as pos_amount")

res.show(5)

#Write Data in Hive
res.write.mode('overwrite').saveAsTable("prod.subscriber_details_staging")

print("subscriber details data written successfully in hive staging")


spark.stop()