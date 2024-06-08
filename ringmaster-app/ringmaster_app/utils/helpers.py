from pathlib import Path
from typing import Optional
from delta.tables import DeltaTable, DeltaTableBuilder
from pyspark.sql import SparkSession, DataFrame


class Helpers:

    @staticmethod
    def read_binary_at(path: Path):
        with path.open("rb") as fb:
            bindata = fb.read()
        return bindata

    @staticmethod
    def generate_delta_table(df: DataFrame,
                             table_name: Optional[str] = "tin_example",
                             description: Optional[str] = "This table stores the tin_example",
                             log_retention: Optional[str] = "interval 30 days",
                             deleted_file_retention: Optional[str] = "interval 1 day",
                             data_skipping_index_cols: Optional[str] = "4",
                             cluster_by_col: Optional[str] = "date"
                             ) -> DeltaTable:
        """
        This method will generate a new DeltaTable only if it doesn't already exist
        :param df: The dataframe to steal the table schema from
        :param table_name: The name of the table
        :param description: The description of the table that lives in the Table metadata
        :param log_retention: How long to store transaction log entries
        :param deleted_file_retention: How long to wait before permanently deleting older files from older Snapshots
        :param data_skipping_index_cols: How many columns to index. The default is 32, but it is better to selectively index
        :param cluster_by_col: Which column do you want cluster by on? (date) is the default here. This column must have stats index
        :return: The DeltaTable instance
        """
        spark = SparkSession.getActiveSession
        dtb: DeltaTableBuilder = (
            DeltaTable.createIfNotExists(spark)
            .tableName(table_name)
            .addColumns(df.schema)
            .comment(description)
            .clusterBy(cluster_by_col)
            .property("description", description)
            .property("delta.logRetentionDuration", log_retention)
            .property("delta.deletedFileRetentionDuration", deleted_file_retention)
            .property("delta.dataSkippingNumIndexedCols", data_skipping_index_cols)
            .property("delta.checkpoint.writeStatsAsStruct", "true")
            .property("delta.checkpoint.writeStatsAsJson", "false")
            .property("delta.checkpointPolicy", "v2")
            .property("delta.enableDeletionVectors", "true")
        )
        # can intercept here if you wanted to pass a func to do more with the DeltaTableBuilder

        return dtb.execute()
