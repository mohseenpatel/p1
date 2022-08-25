from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\Users\\Dell\\Desktop\\Python\\Pandas\\datasets\\AAPLData1.csv"
drdd=sc.textFile(data)
res=drdd.map(lambda x:x.split(",")).toDF(["Open","High","Low","Close","Volume"])
res.show()