from pyspark.sql import SparkSession
import re

# Tạo Spark session
spark = SparkSession.builder \
    .appName("ETL Process") \
    .getOrCreate()

# Đường dẫn đến thư mục chứa các file txt trong Hadoop
hadoop_path = "hdfs://localhost:9000/data_lake/1"

# Đọc tất cả các file txt từ Hadoop
raw_data = spark.read.text(hadoop_path)

# Hiển thị một số dòng dữ liệu để kiểm tra
raw_data.show(100, truncate=False)
