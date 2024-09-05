from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType
import re
from datetime import datetime, timedelta

# Tạo Spark session
spark = SparkSession.builder \
    .appName("ETL Process") \
    .getOrCreate()

# Đường dẫn đến thư mục chứa các file txt trong Hadoop
hadoop_path = "hdfs://localhost:9000/data_lake/1"

# Đọc tất cả các file txt từ Hadoop
raw_data = spark.read.text(hadoop_path)

# Hàm UDF để trích xuất thông tin từ dòng văn bản
def parse_line(line):
    data = {}
    for entry in line.split('\n'):
        if ": " in entry:
            key, value = entry.split(": ", 1)
            data[key] = value
    return data

# Đăng ký UDF
parse_line_udf = udf(parse_line, StringType())

# Chuyển đổi dữ liệu thành DataFrame có cấu trúc
data_rdd = raw_data.rdd.map(lambda r: parse_line(r[0])).filter(lambda d: d)  # Chuyển đổi RDD và lọc các bản ghi hợp lệ
data_df = spark.createDataFrame(data_rdd)

# Hàm UDF để tính toán ngày đăng bài từ "Listed Since"
def calculate_posted_date(listed_since):
    days_ago = int(re.findall(r'\d+', listed_since)[0])
    posted_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    return posted_date

# Đăng ký UDF
calculate_posted_date_udf = udf(calculate_posted_date, StringType())

# Áp dụng UDF để tính toán ngày đăng bài và thêm cột "Posted Date"
transformed_df = data_df.withColumn("Posted Date", calculate_posted_date_udf(col("Listed Since")))

# Chọn các cột cần thiết và sắp xếp theo ngày đăng bài
final_df = transformed_df.select(
    col("Title"),
    col("Cash Price"),
    col("Finance Price"),
    col("Finance Details"),
    col("Exterior"),
    col("Interior"),
    col("Mileage"),
    col("Fuel Type"),
    col("MPG"),
    col("Transmission"),
    col("Drivetrain"),
    col("Engine"),
    col("Location"),
    col("VIN"),
    col("Stock Number"),
    col("Features"),
    col("Posted Date")
).orderBy(col("Posted Date"))

# Hiển thị một số dòng dữ liệu để kiểm tra
final_df.show(100, truncate=False)

# Ghi dữ liệu vào Hive (phân vùng theo ngày)
output_path = "/data_lake/car_data"
final_df.write.mode("append").partitionBy("Posted Date").format("parquet").save(output_path)

# Dừng Spark session
spark.stop()
