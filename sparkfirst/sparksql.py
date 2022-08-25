from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

sc=spark.sparkContext
data="D:\\bigdata\\drivers-20220727T064639Z-001\\drivers\\asl.csv"
drdd=sc.textFile(data)
res=drdd.map(lambda x:x.split(",")).toDF(["name","age","city"])
res.show()

