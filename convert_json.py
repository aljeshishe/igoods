from pyspark.sql import SparkSession
import sys

from pyspark.sql.functions import when, round
from pyspark.sql.types import FloatType, IntegerType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    file_name = sys.argv[1]
    c = spark.read.json(file_name)
    print(f'Read {c.count()} records')
    c.printSchema()

    c = c.drop('data-group-quantity', 'data-energy-drink', 'data-amount', 'data-item-weight', 'data-energy-drink'). \
        withColumn('old_price', c['data-old-price'].cast(FloatType())). \
        withColumn('old_price_per_kg', c['data-old-price-per-kg'].cast(FloatType())). \
        withColumn('price', c['data-price'].cast(FloatType())). \
        withColumn('price_per_kg', c['data-price-per-kg'].cast(FloatType())). \
        withColumn('product_id', c['data-product-id'].cast(IntegerType())). \
        withColumn('type', c['data-type'].cast(StringType())). \
        withColumn('weight', c['data-weight'].cast(IntegerType()))

    calc_discount = round((1 - c.price / c.old_price) * 100, 2)

    c = c.withColumn('discount', when(c.old_price.isNull(), None).otherwise(calc_discount)). \
        select('shop', 'name', 'weight', 'price', 'old_price', 'price_per_kg',
               'old_price_per_kg', 'discount', 'cat', 'product_id', 'type', 'datetm', 'url')
    c.sort('name').show()

    c.write.mode('append').parquet('data.parquet')
