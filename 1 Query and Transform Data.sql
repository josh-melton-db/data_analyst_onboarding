-- Databricks notebook source
-- DBTITLE 1,Import Libraries
-- MAGIC %python
-- MAGIC pip install dbldatagen

-- COMMAND ----------

-- DBTITLE 1,Run Setup, Get Table Name
-- MAGIC %python
-- MAGIC from utils.onboarding_setup import get_config, reset_tables, iot_data_generator, defect_data_generator
-- MAGIC config = get_config(spark)
-- MAGIC reset_tables(spark, config, dbutils)
-- MAGIC iot_data = iot_data_generator(spark, config['rows_per_run'])
-- MAGIC iot_data.write.mode('overwrite').saveAsTable(config['bronze_table'])
-- MAGIC defect_data = defect_data_generator(spark, iot_data)
-- MAGIC defect_data.write.mode('overwrite').saveAsTable(config['defect_table'])
-- MAGIC print(f'YOUR IOT TABLE NAME IS: \n{config["bronze_table"]}')
-- MAGIC print(f'\nYOUR DEFECT TABLE NAME IS: \n{config["defect_table"]}')

-- COMMAND ----------

-- DBTITLE 1,Query the IOT Data
-- Run the first two cells to set up your data! 
select *
from josh_melton_onboarding.sensor_bronze -- <- Paste your iot table name here (printed in the previous cell)

-- COMMAND ----------

-- DBTITLE 1,Query the Defect Data
-- Tip: Shift+Enter runs the selected cell
select * 
from josh_melton_onboarding.defect_bronze -- <- Paste your defect table name here (printed with iot data)

-- COMMAND ----------

-- DBTITLE 1,Created Joined Temp View
-- Create a view joining the two tables together
create or replace view joined_iot_defect -- <- rename the table with your username prefix from before here
as (
  select bronze.*,
         defect.* except(device_id, timestamp)
  from josh_melton_onboarding.sensor_bronze bronze
  left join josh_melton_onboarding.defect_bronze defect
    on bronze.device_id = defect.device_id 
    and bronze.timestamp = defect.timestamp
)

-- COMMAND ----------

-- Add some transformations to the view we created above 
-- and create temp view for future queries in this notebook
create or replace temp view test_2 -- <- rename the table with your username prefix here
as (
  select *, concat(device_id, ":", factory_id) as composite_id, temperature as temp_fahrenheit
  from joined_iot_defect iot
  where factory_id is not null and device_id is not null
)

-- COMMAND ----------

create or replace view test_jlm_1
as (
  select bronze.*,
         defect.* except(device_id, timestamp)
  from josh_melton_onboarding.sensor_bronze bronze
  left join josh_melton_onboarding.defect_bronze defect
    on bronze.device_id = defect.device_id 
    and bronze.timestamp = defect.timestamp
)

-- COMMAND ----------


