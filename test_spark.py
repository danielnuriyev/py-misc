from pyspark.sql import SparkSession

# Path to your Athena JDBC driver
jdbc_driver_path = "/Users/daniel.nuriyev/projects/test-py/athena-jdbc-3.2.2-with-dependencies.jar"

# Athena connection details
athena_url = "jdbc:awsathena://AwsRegion=us-east-1;S3OutputLocation=s3://ss-bi-datalake-query-results/basic-query/daniel-san/;"

# Set up Spark session
spark = SparkSession.builder \
    .appName("AthenaJDBCConnection") \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .getOrCreate()

# Create DataFrame by reading from Athena via JDBC
athena_df = spark.read.format("jdbc") \
    .option("url", athena_url) \
    .option("driver", "com.amazon.athena.jdbc.AthenaDriver") \
    .option("dbtable", "(select * from datalake_agg.uc_countries) as t") \
    .option("CredentialsProvider", "DefaultChain") \
    .load()

# Show the results
athena_df.show()
