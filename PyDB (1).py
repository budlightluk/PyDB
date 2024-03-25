#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('pip install pyspark')


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs_and_uncategorized_products(spark, products_df, categories_df):
    joined_df = products_df.join(categories_df, products_df.product_id == categories_df.product_id, how='left_outer')
    
    uncategorized_products_df = joined_df.filter(col('categories_df.product_id').isNull()) \
                                         .select(products_df.product_name.alias('product_name'))

    product_category_pairs_df = joined_df.select(products_df.product_name.alias('product_name'), 
                                                 categories_df.category_name.alias('category_name'))
    
    return product_category_pairs_df, uncategorized_products_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ProductCategoryPairs") \
        .getOrCreate()

    products_data = [("1", "Product A"),
                     ("2", "Product B"),
                     ("3", "Product C")]

    categories_data = [("1", "Category X", "1"),
                       ("2", "Category Y", None),
                       ("3", "Category Z", "1"),
                       ("4", "Category W", "2")]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name", "product_id"])

    product_category_pairs, uncategorized_products = get_product_category_pairs_and_uncategorized_products(spark, products_df, categories_df)
    
    print("Product-Category pairs:")
    product_category_pairs.show()

    print("Uncategorized Products:")
    uncategorized_products.show()

    spark.stop()


# In[ ]:




