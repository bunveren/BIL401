from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
print("sparksession baslatiliyor...")
spark = SparkSession.builder \
    .appName("minimalSpark") \
    .getOrCreate()
print("sparksession baslatildi!")


csv_file_path = "data/train_properly_quoted.csv" 
my_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Body", StringType(), True),
    StructField("Tags", StringType(), True),
    StructField("CreationDate", StringType(), True), 
    StructField("Y", IntegerType(), True) 
])

print(f"'{csv_file_path}' dosyasi tanimlanan sema ile okunuyor...")
df = spark.read.csv(csv_file_path, header=True, schema=my_schema)
print("dosya okundu.")
df.printSchema()
print("\ntoplam satir s.:", df.count())
print("\ntemel istatistikler:",df.describe().show())
print("\ndegisken dengesi:", df.groupBy("Y").count().show())