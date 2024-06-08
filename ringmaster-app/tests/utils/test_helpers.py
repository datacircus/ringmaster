import unittest

from pyspark.sql import SparkSession
from ringmaster_app.utils.helpers import Helpers
class TestHelpers(unittest.TestCase):
    spark_session: SparkSession
    @classmethod
    def spark(cls) -> SparkSession:
        spark: SparkSession = (
            SparkSession.builder
            .master("local[*]")
            #.config("spark.jars.ivy", "/tmp/ivy2")
            #.config("spark.ivy.home", "/tmp")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .appName("ringmaster_utils_helpers")
            .getOrCreate()
        )
        return spark

    @classmethod
    def setUp(cls):
        cls.spark_session = cls.spark()

    @classmethod
    def tearDownClass(cls):
        cls.spark_session.stop()

    def test_generate_table(self):
        spark = self.spark_session.newSession()

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:29092")
            .option("subscribe", "coffeeco.v1.orders")
            .option("failOnDataLoss", "true")
            .option("mode", "FAIL_FAST")
            .option("startingOffsets", "earliest")
            .option("fetchOffset.retryIntervalMs", "10")
            .option("groupIdPrefix", "ringmaster_unit_helpers")
            .load()
        )

        h = ""



