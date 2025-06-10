import os
import re
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, when, isnull, concat
from pyspark.sql.types import StringType, IntegerType, FloatType

import os
from pyspark.sql import SparkSession

# 设置 Python 解释器路径
# os.environ["PYSPARK_PYTHON"] = ""
os.environ["PYSPARK_PYTHON"] = "D:\BigData\Py\softwore\python.exe"
# 创建SparkSession
spark = SparkSession.builder \
    .appName("Links Data Preprocessing") \
    .getOrCreate()

# 定义文件路径
input_file = "../ml-latest-small/links.csv"
backup_file = "links_backup.csv"

# 备份原始文件
if os.path.exists(input_file):
    shutil.copy2(input_file, backup_file)
    print(f"已备份原始文件到: {backup_file}")
else:
    print(f"错误: 找不到输入文件 {input_file}")
    exit(1)

# 备份原始文件
if os.path.exists(input_file):
    shutil.copy2(input_file, backup_file)
    print(f"已备份原始文件到: {backup_file}")
else:
    print(f"错误: 找不到输入文件 {input_file}")
    exit(1)

# 读取CSV文件
links_df = spark.read.csv(input_file, header=True, inferSchema=True)

# 检查数据结构
print("原始数据结构:")
links_df.printSchema()

# 数据类型转换和格式标准化
# 确保movieId是整数类型
links_df = links_df.withColumn("movieId", col("movieId").cast(IntegerType()))

# 处理imdbId (标准化为不带"tt"前缀的数字格式)
def normalize_imdb_id(imdb_id):
    if imdb_id is None:
        return None
    # 移除可能的前导"tt"
    imdb_id_str = str(imdb_id).replace("tt", "")
    # 转换为整数以去除前导零
    try:
        return int(imdb_id_str)
    except ValueError:
        return None

normalize_imdb_udf = udf(normalize_imdb_id, IntegerType())
links_df = links_df.withColumn("imdbId", normalize_imdb_udf(col("imdbId")))

# 处理tmdbId (确保是整数类型)
links_df = links_df.withColumn("tmdbId", col("tmdbId").cast(IntegerType()))

# 保存处理后的数据回源文件
temp_dir = "temp_links_csv"
links_df.write.csv(temp_dir, header=True, mode="overwrite")

# 查找并复制生成的CSV文件
csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
if csv_files:
    csv_file = os.path.join(temp_dir, csv_files[0])
    shutil.copy2(csv_file, input_file)
    print(f"已将处理后的数据保存回: {input_file}")
else:
    print("错误: 未找到生成的CSV文件")

# 清理临时目录
shutil.rmtree(temp_dir)

# 停止SparkSession
spark.stop()
