from pyspark.sql import SparkSession

print("sparksession baslatiliyor...")
spark = SparkSession.builder \
    .appName("minimalSpark") \
    .getOrCreate()
print("sparksession baslatildi!")

csv_file_path = "data/train.csv" 

try:
    print(f"'{csv_file_path}' dosyasi okunuyor...")
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    print("dosya okundu.")
    print("dataframe:")
    df.printSchema()
    print("\nilk 5 satir:")
    df.show(5, truncate=False) 
    print("\ntoplam satir s.:", df.count())

except Exception as e:
    print(f"HATA: {e}")

finally:
    print("\nsession sonlandiriliyor...")
    spark.stop()
    print("session sonlandirildi.")