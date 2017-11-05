# AvroTools

A collection of tools for processing Avro files via mapreduce.

### ConcatAvroFiles

Recursively reads an input directory of avro files that have an externally specified schema attached to them and concatenates them into a single output file.

Usage:

`yarn jar AvroConcat.jar ConcatAvroFiles <inpath> <outpath> <schema.avsc>`

Use the hive table to examine the data in the new Avro file.
