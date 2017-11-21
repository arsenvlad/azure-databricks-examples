// Databricks notebook source
// MAGIC %sh
// MAGIC ls
// MAGIC lsblk
// MAGIC df -h

// COMMAND ----------

// Set Azure Storage Account key
spark.conf.set("fs.azure.account.key.STORAGE_ACCOUNT.blob.core.windows.net","ACCOUNT_KEY")

// COMMAND ----------

// Set Azure Storage Account SAS
spark.conf.set( "fs.azure.sas.CONTAINER.STORAGE_ACCOUNT.blob.core.windows.net", "SAS_TOKEN_FOR_CONTAINER") 

// COMMAND ----------

display(dbutils.fs.ls("wasbs://CONTAINER@STORAGE_ACCOUNT.blob.core.windows.net/example/data"))

// COMMAND ----------

var csv = sqlContext.read.format("csv")
  .option("header","false")
  .load("wasbs://CONTAINER@STORAGE_ACCOUNT.blob.core.windows.net/Encoding Time.csv")

// COMMAND ----------

// Read as parquet
val parquet = spark.read.parquet("wasbs://CONTAINER@STORAGE_ACCOUNT.blob.core.windows.net/example/data/people.parquet")
// Write out as CSV
parquet.write.format("com.databricks.spark.csv").option("header","true").save("wasbs://CONTAINER@STORAGE_ACCOUNT.blob.core.windows.net/example/data/people.csv")

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type","ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id","")
spark.conf.set("dfs.adls.oauth2.credential","")
spark.conf.set("dfs.adls.oauth2.refresh.url","https://login.windows.net/TENANT_ID/oauth2/token")

// COMMAND ----------

display(dbutils.fs.ls("adl://ACCOUNT_NAME.azuredatalakestore.net/databricks/"))

// COMMAND ----------

