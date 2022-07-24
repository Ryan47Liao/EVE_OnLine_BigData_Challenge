#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import statements
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
#
from datetime import datetime
import seaborn as sns
#Window
from pyspark.sql.window import Window
#misc
import functools as fc
import matplotlib.pyplot as plt
import pandas as pd

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import seaborn as sns


# In[2]:


get_ipython().system('gsutil ls gs://bigdataplatform-order/')


# In[3]:


spark = SparkSession.builder.appName('Eve_online').getOrCreate()


# In[4]:


get_ipython().run_line_magic('load_ext', 'google.cloud.bigquery')


# In[5]:


get_ipython().run_cell_magic('bigquery', '', 'SELECT COUNT(*) as total_rows\nFROM `proud-apogee-348022.market.order`')


# In[19]:


get_ipython().run_cell_magic('bigquery', '', 'SELECT *\nFROM `proud-apogee-348022.market.order`\nLIMIT 10')


# In[90]:


get_ipython().run_cell_magic('bigquery', '', 'SELECT *\nFROM `proud-apogee-348022.market.order`\nWHERE is_buy_order = True\nLIMIT 10')


# In[20]:


get_ipython().run_cell_magic('bigquery', '', 'SELECT type_id, sum(price) as total price, COUNT(*) as orders\nFROM `proud-apogee-348022.market.order`\nWHERE is_buy_order = True\nGROUP BY type_id\nLIMIT 10')


# In[8]:


from google.cloud import bigquery
client = bigquery.Client()


# In[35]:


sql = """
SELECT region_id, COUNT(*) as orders
FROM `proud-apogee-348022.market.order`
WHERE is_buy_order = True
GROUP BY region_id
"""


# In[36]:


df = client.query(sql).to_dataframe()


# In[38]:


df.head(10)


# In[29]:


dfregions = spark.read.csv("gs://bigdataplatform-order/Region_Names.csv", header=True, inferSchema=True).toPandas()


# In[31]:


dfregions.rename(columns={"ID": "region_id"}, inplace = True)


# In[53]:


df_region_orders =  pd.merge(dfregions, df, on="region_id")


# In[63]:


df_region_orders.sort_values(by = 'orders', ascending = False, inplace=True)


# In[65]:


sns.set_theme(style="whitegrid")
f, ax = plt.subplots(figsize=(15, 15))
sns.barplot(x="orders", y="Name", data=df_region_orders,
            label="Region", color="b", orient='h')


# In[72]:


sql = """
SELECT type_id, COUNT(*) as orders, SUM(price) as total_price
FROM `proud-apogee-348022.market.order`
WHERE is_buy_order = True
GROUP BY type_id
"""


# In[73]:


df1 = client.query(sql).to_dataframe()


# In[77]:


df1.head(1)


# In[27]:


df_items = spark.read.csv("gs://bigdataplatform-order/Commodity_Names.csv", header=True, inferSchema=True).toPandas()


# In[28]:


df_items.head(1)


# In[30]:


df_items.rename(columns={"ID": "type_id"}, inplace = True)


# In[80]:


df_item_demand =  pd.merge(df_items, df1, on="type_id")


# In[81]:


df_item_demand.head(2)


# In[82]:


df_item_demand.sort_values(by = 'orders', ascending = False, inplace=True)


# In[83]:


df_item_demand_top20 = df_item_demand.head(20)
sns.set_theme(style="whitegrid")
f, ax = plt.subplots(figsize=(15, 15))
sns.barplot(x="orders", y="Name", data = df_item_demand_top20,
            label="Region", color="b", orient='h')


# In[84]:


df_item_demand['price_per_order'] = df_item_demand['total_price'] / df_item_demand['orders']


# In[85]:


df_item_demand.head(2)


# In[86]:


df_item_demand.sort_values(by = 'price_per_order', ascending = False, inplace=True)


# In[87]:


df_item_demand_top20 = df_item_demand.head(20)
sns.set_theme(style="whitegrid")
f, ax = plt.subplots(figsize=(15, 15))
sns.barplot(x="price_per_order", y="Name", data = df_item_demand_top20,
            label="Region", color="b", orient='h')


# In[8]:


sql = """
SELECT region_id, type_id , COUNT(*) as orders, SUM(price) as total_price
FROM `proud-apogee-348022.market.order`
WHERE is_buy_order = True
GROUP BY region_id, type_id
"""


# In[9]:


df2 = client.query(sql).to_dataframe()


# In[10]:


df2.head(2)


# In[11]:


df2['per_order_price'] = df2['total_price'] / df2['orders']


# In[18]:


df_max = df2.groupby(['type_id'])['per_order_price'].max().reset_index()


# In[19]:


df_max.head(2)


# In[20]:


df_max.rename(columns={"per_order_price": "max_price"}, inplace = True)


# In[40]:


df_normalize = pd.merge(df2, df_max,  how='left', left_on=['type_id'], right_on = ['type_id'])


# In[41]:


df_normalize.head(20)


# In[42]:


df_normalize['price_index'] = df_normalize['per_order_price']/df_normalize['max_price']


# In[60]:


df_items.head(2)


# In[57]:


df_normalize_final =  pd.merge(df_normalize, df_items, on="type_id")
df_normalize_final =  pd.merge(df_normalize_final, dfregions, on="region_id")


# In[56]:


del df_normalize_final


# In[62]:


df_final_index = df_normalize_final[['Name_y','Name_x', 'price_index', 'orders']]


# In[64]:


df_final_index.rename(columns={"Name_y": "region", 'Name_x': 'item'}, inplace = True)


# In[66]:


df_final_index.sort_values(by = 'orders', ascending = False, inplace=True)


# In[69]:


df_final_index.head(10)


# In[7]:


get_ipython().run_cell_magic('bigquery', '', 'SELECT COUNT(*) as total_rows\nFROM `proud-apogee-348022.market.order`\nWHERE is_buy_order is True')


# In[14]:


sql = """
SELECT * as orders
FROM `proud-apogee-348022.market.order`
WHERE is_buy_order = True
"""


# In[13]:


df_allorders.shape


# In[ ]:





# In[ ]:





# In[8]:


df0801 = spark.read.csv("gs://bigdataplatform-order/Data-Order/bdp-order/20210801.csv", header=True, inferSchema=True)

