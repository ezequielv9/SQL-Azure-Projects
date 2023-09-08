# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "3c648131-21c5-42f5-94ab-414e579317f3",
"fs.azure.account.oauth2.client.secret": 'AW-8Q~FRVWOn64ATcXfwHpord6_BgNTzJnaDFa9V',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b57895eb-8ff3-4412-9e22-ea9535b8a106/oauth2/token"}


dbutils.fs.mount(
source = "abfss://nintendo-games@nintendodata.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/nintendo-games",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/nintendo-games"

# COMMAND ----------

spark

# COMMAND ----------

nintendogames = spark.read.format ("csv").option("header","true").load("/mnt/nintendo-games/raw-data/nintendogames.csv")


# COMMAND ----------

nintendogames.show()

# COMMAND ----------

nintendogames.printSchema()

# COMMAND ----------


