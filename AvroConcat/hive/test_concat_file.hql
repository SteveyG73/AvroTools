CREATE EXTERNAL TABLE `test_concat_file`
ROW FORMAT SERDE `org.apache.hadoop.hive.serde2.avro.AvroSerDe`
STORED AS AVRO
LOCATION '<outpath>'
TBLPROPERTIES ('avro.schema.url'='<schema.avsc>');
