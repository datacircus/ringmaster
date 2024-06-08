import unittest

from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.protobuf.functions import from_protobuf
from ringmaster_app.utils.helpers import Helpers


class TestHelpers(unittest.TestCase):
    spark_session: SparkSession

    test_resource_dir = Path('__FILE__').parent.parent.joinpath("resources")
    @classmethod
    def spark(cls) -> SparkSession:
        spark: SparkSession = (
            SparkSession.builder
            .master("local[*]")
            .config("spark.driver.extraJavaOptions",
                    "-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.2.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.apache.spark:spark-protobuf_2.12:3.5.1")
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

    def test_decode_protobuf(self):
        spark = self.spark_session.newSession()

        # get the path to the tin table
        tin_table_path = self.test_resource_dir.joinpath("coffeeco_v1_orders_tin").absolute()

        # get the DeltaTable representation
        dt: DeltaTable = DeltaTable.forPath(spark, tin_table_path.as_posix())
        # note. The tin table is struct<key:binary,value:binary,timestamp:TimestampType,date:DateType>

        # load the coffeeco.bin descriptor
        descriptor_file = self.test_resource_dir.joinpath("coffeecov1", "descriptor.bin").absolute()
        coffeecov1_bin = Helpers.read_binary_at(descriptor_file)

        coffee_orders_df = dt.toDF().select(
            "date", "timestamp",
            from_protobuf(
                data=col("value"),
                messageName="coffeeco.v1.Order",
                options={"mode": "FAILFAST"},
                binaryDescriptorSet=coffeecov1_bin
            ).alias("order"),
        )







