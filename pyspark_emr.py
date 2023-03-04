from pyspark.sql import SparkSession
from pyspark.sql import col

S3_input='s3://entpric-dp-regional-pricing/region_zip_mapping/'

#S3_output='s3://entpric-dp-regional-pricing/region_zip_mapping/'

def main():
    spark = SparkSession.builder.appName("hello").getOrCreate()
    all_data = spark.read.csv(S3_input, header=True)
    print(f"count is {all_data.count()}")


if __name__ == '__main__':
    main()