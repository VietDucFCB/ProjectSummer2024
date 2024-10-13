from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, expr, regexp_replace, trim, concat_ws, collect_list, sum as F_sum
from pyspark.sql import Window

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Extract_Car_Data") \
    .getOrCreate()

data_lake_path = "hdfs://localhost:9000/data_lake/data_crawled/"

df_raw = spark.read.option("recursiveFileLookup", "true").text(data_lake_path)

# Gộp các dòng liên quan dựa trên logic
# Giả sử mỗi mục xe có một dòng 'Title:' - Dùng dòng này để xác định bắt đầu mỗi mục.
window_spec = Window.orderBy("value")
df_raw = df_raw.withColumn("is_title", df_raw['value'].contains('Title:').cast('int'))
df_raw = df_raw.withColumn("group", F_sum('is_title').over(window_spec))

# Gộp các dòng thành một dòng cho mỗi xe
df_combined = df_raw.groupBy("group").agg(concat_ws(' ', collect_list('value')).alias("value"))

# Định nghĩa các biểu thức chính quy để trích xuất các trường từ văn bản
patterns = {
    'Title': r'Title: (.*)',
    'Cash Price': r'Cash Price: \$(\d+,\d+)',
    'Finance Price': r'Finance Price: \$(\d+)/moEstimate',
    'Finance Details': r'Finance Details: (.*)',
    'Exterior': r'Exterior: (.*)',
    'Interior': r'Interior: (.*)',
    'Mileage': r'Mileage: ([\d,]+) miles',
    'Fuel Type': r'Fuel Type: (.*)',
    'MPG': r'MPG: (\d+ city / \d+ highway)',
    'Transmission': r'Transmission: (.*)',
    'Drivetrain': r'Drivetrain: (.*)',
    'Engine': r'Engine: (.*)',
    'Location': r'Location: (.*)',
    'Listed Since': r'Listed Since: (.*)',
    'VIN': r'VIN: (.*)',
    'Stock Number': r'Stock Number: (.*)',
}

# Tạo DataFrame mới bằng cách trích xuất các trường dựa trên biểu thức chính quy
df = df_combined
for column, pattern in patterns.items():
    df = df.withColumn(column, regexp_extract(col("value"), pattern, 1))

# Trích xuất trường "Features" bằng cách sử dụng `substring_index`
df = df.withColumn("Features", expr("substring_index(substring_index(value, 'Features: ', -1), 'See less', 1)"))

# Loại bỏ cột "value" gốc và cột "group"
df = df.drop("value").drop("group")

#
# Xử lý việc loại bỏ các ký tự xuống dòng và khoảng trắng thừa
columns_to_clean = ['Title', 'Cash Price', 'Finance Price', 'Finance Details', 'Exterior',
                    'Interior', 'Mileage', 'Fuel Type', 'MPG', 'Transmission', 'Drivetrain',
                    'Engine', 'Location', 'Listed Since', 'VIN', 'Stock Number', 'Features']

for column in columns_to_clean:
    df = df.withColumn(column, regexp_replace(col(column), r'[\n\r]+', ' '))
    df = df.withColumn(column, trim(col(column)))

# Đường dẫn để lưu file CSV đầu ra
output_folder = "C:/Users/kkagi/Downloads/test"

# Ghi DataFrame thành CSV
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)

# Đợi quá trình ghi hoàn tất trước khi tiếp tục
spark.sparkContext.stop()

# Tiếp tục với việc đổi tên file part
import os
import glob

csv_files = glob.glob(f"{output_folder}/part-*.csv")
if csv_files:
    csv_file = csv_files[0]  # Tìm file part CSV

    # Đổi tên thành một tên dễ đọc hơn
    new_csv_file = f"{output_folder}/car_data.csv"
    os.rename(csv_file, new_csv_file)

    print(f"File saved as: {new_csv_file}")
else:
    print("No CSV file found to rename.")
