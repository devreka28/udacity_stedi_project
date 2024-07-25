The STEDI project's aim is to create a table ready for research analysis using source customer and accelerometer data along with data captured by the step trainer app. 

All data is sourced as JSON files in AWS S3 bucket on the top of which Glue tables were built.

The AWS S3 buckets are structured by data sources then one level lower each data source is split to zones which are landing, trusted and curated where applicable. The final output is the JSON file called machine_learning_trusted to be used by the Machine Learning team. This includes all the accelerometer and step trainer data for customers who agreed to share their data for research and who have this data in the first place.

For ETL, AWS Glue graphical inteface was used to generate the Python script describing the required ETL steps.

Please see Glue table names and descriptions below:

Landing
  - Customer: 956 - source data
  - Accelerometer: 81273 - source data
  - Step Trainer: 28680 - source data
Trusted
  - Customer: 482 - customer data agreeing their data to be used for research
  - Accelerometer: 40981 - accelerometer data for only customers who agreed to share it with research
  - Step Trainer: 14460 - Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research
Curated
  - Customer: 482 - customers who have accelerometer data and have agreed to share their data for research 
  - Machine Learning: 43681 - Step Trainer Readings, and the associated accelerometer reading data for the same timestamp for customers who have agreed to share their data
