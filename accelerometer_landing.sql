CREATE EXTERNAL TABLE IF NOT EXISTS `udacity_stedi_project`.`accelerometer_landing` (
  `user` string,
  `x` float,
  `y` float,
  `z` float,
  `timestamp` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacitycoursero3/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
