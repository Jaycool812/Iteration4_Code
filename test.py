import pandas as pd

import os

os.chdir('/Users/jayx/Desktop/infosys722')

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('basics').getOrCreate()

df = spark.read.csv("Dataset722.csv", header=True)
# df.show()


df_latitude = df.select("Latitude")
df_longitude = df.select("Longitude")

# import matplotlib.pyplot as plt
# We can use the describe method get some general statistics on our data too. Remember to show the DataFrame!
# df.describe().show()

# use schema to view the data type
# df.printSchema()
import pyspark_dist_explore

import matplotlib.pyplot as plt

# import pyspark_dist_explore as ex
# fig, axes = plt.subplots(2)
#
# fig.set_size_inches(10, 10)
# ex.hist(axes[0], df_latitude, bins = 20, color=['blue'])
# axes[0].set_title('Latitude Distribution')
# axes[0].legend()
#
# ex.hist(axes[1], df_longitude, bins = 20, color=['blue'])
# axes[1].set_title('Longitude Distribution')
# axes[1].legend()
# plt.show()

# pd = df.toPandas()
# pd_latitude = pd["Latitude"]
# pd_longitude = pd["Longitude"]
# fig, ax1 = plt.subplots(2)
# ax1[0].set_title('Latitude')
# ax1[0].boxplot(pd_latitude)
#
#
# ax1[1].set_title('Longitude')
# ax1[1].boxplot(pd_longitude)
#
# plt.show()

# df.filter("CD1 is null").show()


from pyspark.sql.types import (StructField, IntegerType, StructType, DoubleType)

# Then create a variable with the correct structure.
data_schema = [StructField('ID', IntegerType(), True),
               StructField('Latitude', DoubleType(), True),
               StructField('Longitude', DoubleType(), True),
               StructField('CD1', DoubleType(), True),
               StructField('CD2', DoubleType(), True),
               StructField('CD3', DoubleType(), True),
               StructField('CD4', DoubleType(), True),
               StructField('WD1', DoubleType(), True),
               StructField('WD2', DoubleType(), True),
               StructField('WD3', DoubleType(), True),
               StructField('WD4', DoubleType(), True),
               StructField('BS', DoubleType(), True)]

final_struct = StructType(fields=data_schema)
# %%
# And now we can read in the data using that schema. If we print the schema, we can see that age is now an integer.
df = spark.read.csv("Dataset722.csv", schema=final_struct)

df.printSchema()

df.show()


df_final = df.na.fill("0")
df_final = df_final.drop("WD1", "WD2", "WD3")
pd1 = df_final.toPandas()
pd1["CD1"] = pd1["CD1"].astype(float)


# print(df_final)

# #
# data_CD1 = df_final.select('CD1').show()
#Joining the dataframe
# df_new = spark.read.csv("Plastic2.csv", header=True)
# inner_join = df_final.join(df_new, df_final['ID'] == df_new['ID'])
# inner_join.show()


# data_CD1 = pd1['CD1']
#
# print(type(data_CD1[2]))
#
# new_CD1 = []
# for i in data_CD1:
#     if i < 50000:
#         new_CD1.append(0)
#     elif i >= 50000 or i < 100000:
#         new_CD1.append(1)
#     else:
#         new_CD1.append(2)


#
# pollution_index = pd.DataFrame(new_CD1)
# # concatenate a column to a dataset
# final_dataset = pd.concat([pd1, pollution_index], axis=1)
#
# # Rename one column name as the deafault name is 0
# final_dataset.rename(columns={0: 'Pollution Index'}, inplace=True)
#
# print(final_dataset)


print("hello")