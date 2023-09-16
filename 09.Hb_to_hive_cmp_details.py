#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram



from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hbtohive").master("yarn").enableHiveSupport().getOrCreate()

#Complaint_details
#sid sname cmp id amp regarding 	descr status staff_name

df_sb=spark.read.format("org.apache.phoenix.spark").option("table","SUBSCRIBER_HB").option("zkUrl","localhost:2181").load()
df_cm=spark.read.format("org.apache.phoenix.spark").option("table","COMPLAINT_HB").option("zkUrl","localhost:2181").load()


df_sb.createOrReplaceTempView("Subscriber_tmp2")
df_cm.createOrReplaceTempView("COMPLAINT_tmp2")



df_result=spark.sql("""SELECT
				S.sid as subscriberId,
				S.name as subscribername,
				cm.cmp_id as complaintId,
				cm.regarding as complaintReg,
				cm.descr as description,
				cm.sys_cre_date as com_cre_date,
				cm.sys_upd_date as com_upd_date,
				cm.status as status
				from Subscriber_tmp2 S
				INNER JOIN COMPLAINT_tmp2 cm on S.sid=cm.sid

""")
#df_result.show(5)
#df_result.write.mode('overwrite').saveAsTable("prod.complaint_details_staging")

spark.catalog.dropTempView("Subscriber_tmp2")
spark.catalog.dropTempView("COMPLAINT_tmp2")

print("Data sucessfully Loaded")

############################

#DSL Approch
df_sbb=df_sb.selectExpr("SID","NAME")
c_test=df_cm.join(df_sbb, "sid", how="inner")

c_test.show(5)

#Give Alias
res_cmd=c_test.selectExpr("SID as subscriberId", "NAME as subscribername","CMP_ID as complaintId","REGARDING as complaintReg","DESCR as description","SYS_CRE_DATE as com_cre_date","SYS_UPD_DATE as com_upd_date","STATUS as status")

res_cmd.show(5)

#Write Data in Hive
res_cmd.write.mode('overwrite').saveAsTable("prod.complaint_details_staging")

spark.stop()




