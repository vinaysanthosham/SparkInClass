from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *



print("\nRead Data from File")
df = spark.read.csv(r"C:\Users\vinay\Desktop\BigData\ConsumerComplaints.csv",header= True)
df.show()
grp = df.groupby("Zip Code").count().show()



dist = (df.select('Date Received', 'Product Name', 'Sub Product', 'Issue', 'Sub Issue', 'Consumer Complaint Narrative', 'Company Public Response', 'Company', 'State Name', 'Zip Code', 'Tags', 'Consumer Consent Provided', 'Submitted via', 'Date Sent to Company', 'Company Response to Consumer', 'Timely Response', 'Consumer Disputed', 'Complaint ID').distinct().count())
tot_dist = (df.select('Date Received', 'Product Name', 'Sub Product', 'Issue', 'Sub Issue', 'Consumer Complaint Narrative', 'Company Public Response', 'Company', 'State Name', 'Zip Code', 'Tags', 'Consumer Consent Provided', 'Submitted via', 'Date Sent to Company', 'Company Response to Consumer', 'Timely Response', 'Consumer Disputed', 'Complaint ID').count())

print("Dup Count",tot_dist - dist)

df2 = spark.read.csv(r"C:\Users\vinay\Desktop\BigData\ConsumerComplaints.csv",header= True)
df2.show()

a = df.unionAll(df2).orderBy(df['Company'])
print(a)

df.iloc[13].show()
'''print("\nPrinting Schema of Stored Data")
df.printSchema()
print("\nShows Employee Table")
df.select("employees").show()
#df.select('firstName','lastName').show()


df.createOrReplaceTempView("people")
print("\nShows All Data in Tables")
sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()'''



