from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "D:/bigdata/drivers-20220727T064639Z-001/drivers/asl.csv"
sc=spark.sparkContext
sc.setLogLevel("ERROR")
aslrdd=sc.textFile(data)

res = aslrdd.filter(lambda x: "hyd" in x)

for i in res.collect():
    print(i)