"""Check what exists in MinIO using PySpark S3A listing."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckMinIO")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
jvm = sc._jvm
conf = sc._jsc.hadoopConfiguration()
fs_class = jvm.org.apache.hadoop.fs.FileSystem
uri = jvm.java.net.URI("s3a://curated/")
fs = fs_class.get(uri, conf)

def ls(path):
    try:
        status = fs.listStatus(jvm.org.apache.hadoop.fs.Path(path))
        return [(s.getPath().toString(), s.getLen()) for s in status]
    except Exception as e:
        return [("ERROR: " + str(e), 0)]

print("=== s3a://curated/ ===")
for p, sz in ls("s3a://curated/"):
    print(f"  {p}  ({sz} bytes)")

print("=== s3a://curated/trips/ ===")
for p, sz in ls("s3a://curated/trips/"):
    print(f"  {p}  ({sz} bytes)")

spark.stop()
