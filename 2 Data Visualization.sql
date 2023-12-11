-- Databricks notebook source
-- MAGIC %md
-- MAGIC Now that we've transformed our data into something interesting and saved it to a table, let's begin visualizing it! </br></br> Run the cell below and click the + button next to Table as shown in the image below, and create a bar chart visualization with the data. Make the X axis the factory id, the y axis the count, and group by defect. Click on the visualization's title to rename it to something more descriptive  </br></br>
-- MAGIC
-- MAGIC <img style="float: right" width="500" src="https://github.com/josh-melton-db/data_analyst_onboarding/blob/main/utils/images/create_visual.png?raw=true">

-- COMMAND ----------

select *
from onboarding.sensor_silver  -- <- change to the table created in the previous notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click the down arrow next to the visualization's title and select "add to dashboard" and "add to new dashboard"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click the + again and this time create a line chart visualization. Set the X axis to avg_heating_rate, y axis to sum of count, and group by defect. See if there are any spikes in defects for certain values of heating rate. </br></br>
-- MAGIC Next let's add this to the dashboard you made before - click the down arrow next to the visualization's title, "add to dashboard", and select the dashboard you made in the last step

-- COMMAND ----------

select *
from onboarding.josh_melton_sensor_bronze  -- <- change to the table created in the previous notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Try creating another visualization with the raw bronze data and adding that to the dashboard as well. Now you can share various insights into your data directly from Databricks! Click the "share" button in the top right of the dashboard to share with other groups or users. Click "schedule" to run the notebook and automatically update the dashboard
