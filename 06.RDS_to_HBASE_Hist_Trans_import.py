#--Name : RTB
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDStoHDFS_Trans_Table").master("local").getOrCreate()

host="jdbc:postgresql://database-1.cuqahmcd1hmu.ap-south-1.rds.amazonaws.com:5432/dev"
user="myuser"
pwd="mypassword"
driver="org.postgresql.Driver"

print("Historical Data import for transactional table is started....")

#SUBSCRIBER Table
df_sb=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","SUBSCRIBER").load()
df_sb.write.format("org.apache.phoenix.spark").option("table","SUBSCRIBER_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("SUBSCRIBER Table Imported successfully")

#ADDRESS Table
df_ad=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","ADDRESS").load()
df_ad.write.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("ADDRESS Table Imported successfully")

#STAFF Table
df_sf=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","STAFF").load()
df_sf.write.format("org.apache.phoenix.spark").option("table","STAFF_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("STAFF Table Imported successfully")

#COMPLAINT Table
df_cm=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","COMPLAINT").load()
df_cm.write.format("org.apache.phoenix.spark").option("table","COMPLAINT_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("COMPLAINT Table Imported successfully")

print("All Historical data of transactional table imported successfully")

#df.createOrReplaceTempView("tab1")
#df2=spark.sql("select ct_id,ct_name from tab1")
#df.write.format("csv").save("/user/sagar/test1")
#df.show(5)

spark.stop()