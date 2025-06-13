from pyspark.sql import SparkSession

print("sparksession baslatiliyor...")
spark = SparkSession.builder \
    .appName("minimalSpark") \
    .getOrCreate()
print("sparksession baslatildi!")

csv_file_path = "data/train_properly_quoted.csv" 

try:
    print(f"'{csv_file_path}' dosyasi okunuyor...")
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
          
    #df = spark.read.csv(csv_file_path, header=True, inferSchema=True, sep=',', quote=None)
    #df = spark.read.csv(csv_file_path, header=True, inferSchema=True, sep='\t')
    #df = spark.read.csv(csv_file_path, header=True, inferSchema=True, sep=',', quote="'")
    #df = spark.read.csv(csv_file_path, header=True, inferSchema=True, sep=',', quote=None)
    
    print("dosya okundu.")
    print("dataframe:")
    df.printSchema()
    #print("\nilk 5 satir:")
    #df.show(5, truncate=False) 
    print("\ntoplam satir s.:", df.count())
    print("\ntemel istatistikler:",df.describe().show())
    print("\ndegisken dengesi:", df.groupBy("Y").count().show())

except Exception as e:
    print(f"HATA: {e}")

finally:
    print("\nsession sonlandiriliyor...")
    spark.stop()
    print("session sonlandirildi.")