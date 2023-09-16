#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
import datetime
spark = SparkSession.builder.appName("Total_revenue_report").master("yarn").enableHiveSupport().getOrCreate()

currentdate = datetime.datetime.now().strftime("%Y-%m-%d")

print("Revenue generation extraction job started")

revenue_report=spark.sql("""
					SELECT 
					SD.country as Country,
					count(SD.subscriberId) as Total_Subscriber,
					sum(coalesce(pre_amount,pos_amount,0)) as total_revenue
					FROM prod.subscriber_details SD
					group by SD.country,Active_flag having SD.Active_flag='A'
					""")


revenue_report.show()

revenue_report.coalesce(1).write.mode("append").format("CSV").option("header","true").option("delimiter","|").save("s3://finalprojectb9/EDWA_Reports/output/"+currentdate)

print("Subscriber revenue extraction job finished sucessfully and data stored in s3")

spark.stop()