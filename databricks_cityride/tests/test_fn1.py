from dfutility_helper.utils import add_map
from pyspark.sql.session import SparkSession

def test_add_map():
    spark = SparkSession.builder.getOrCreate()
    df = spark.range(2).toDF("Id")
    df = add_map(df, p1 = 'test')
    assert 'metadata' in df.columns