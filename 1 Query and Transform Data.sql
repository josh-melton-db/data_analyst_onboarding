-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Query and Transform Data
-- MAGIC Run the first two cells </br>
-- MAGIC Replace the default table names in the queries below with your table names for this exercise

-- COMMAND ----------

-- DBTITLE 1,Run me: Import Libraries
-- MAGIC %python
-- MAGIC pip install dbldatagen

-- COMMAND ----------

-- DBTITLE 1,Run me: Setup, Get Table Name
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
from onboarding.josh_melton_sensor_bronze -- <- Paste your iot table name here (printed in the previous cell)

-- COMMAND ----------

-- DBTITLE 1,Query the Defect Data
-- Tip: Shift+Enter runs the selected cell
select * 
from onboarding.josh_melton_defect_bronze -- <- Paste your defect table name here (printed with iot data)

-- COMMAND ----------

-- DBTITLE 1,Created Joined Temp View
-- Create a view joining the two tables together
create or replace view onboarding.joined_iot_defect -- <- rename the view with your username prefix from before
as (
  select bronze.*,
         defect.* except(device_id, timestamp)
  from onboarding.josh_melton_sensor_bronze bronze
  left join onboarding.josh_melton_defect_bronze defect
    on bronze.device_id = defect.device_id 
    and bronze.timestamp = defect.timestamp
)

-- COMMAND ----------

-- Add some transformations to the view we created above 
-- and create temp view for future queries in this notebook
create or replace temp view test_2 -- <- rename the temp view if you'd like
as (
  select *, 
        concat(device_id, ":", factory_id) as composite_id, 
        temperature/delay as heating_rate,
        temperature as temp_fahrenheit
  from onboarding.joined_iot_defect iot
  where factory_id is not null and device_id is not null
)

-- COMMAND ----------

create or replace table onboarding.sensor_silver  -- <- rename the table with your username prefix from before
as (
  select factory_id, defect, count(*) as count, avg(heating_rate) as avg_heating_rate
  from test_2
  group by defect, factory_id
)

-- COMMAND ----------

select *
from onboarding.sensor_silver  -- <- change to the table created in the cell above

-- COMMAND ----------


