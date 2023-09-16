#--Name : RTB
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDStoHBASE_Ref_Table").master("yarn").getOrCreate()

host="jdbc:postgresql://database-1.cuqahmcd1hmu.ap-south-1.rds.amazonaws.com:5432/dev"
user="myuser"
pwd="mypassword"
driver="org.postgresql.Driver"

print("Data import for reference table is started....")
#Country Table
df_cn=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","COUNTRY").load()

df_cn.write.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("Country Table Imported successfully")

#City Table
df_ct=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CITY").load()
df_ct.write.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("City Table Imported successfully")

#PLAN_PREPAID Table
df_ppr=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","PLAN_PREPAID").load()
df_ppr.write.format("org.apache.phoenix.spark").option("table","PLAN_PREPAID_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("PLAN_PREPAID Table Imported successfully")

#PLAN_POSTPAID
df_ppo=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","PLAN_POSTPAID").load()
df_ppo.write.format("org.apache.phoenix.spark").option("table","PLAN_POSTPAID_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("PLAN_POSTPAID Table Imported successfully")

print("All reference data imported sucessfully")

#df.createOrReplaceTempView("tab1")
#df2=spark.sql("select ct_id,ct_name from tab1")
#df.write.format("csv").save("/user/sagar/test1")
#df.show(5)

spark.stop()