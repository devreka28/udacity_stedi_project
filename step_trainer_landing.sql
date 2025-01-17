CREATE EXTERNAL TABLE IF NOT EXISTS `udacity_stedi_project`.`step_trainer_landing` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacitycoursero3/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
