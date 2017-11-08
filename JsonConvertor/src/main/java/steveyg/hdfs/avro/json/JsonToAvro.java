package steveyg.hdfs.avro.json;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import steveyg.hdfs.utils.MapReduceUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;


public class JsonToAvro extends Configured implements Tool{


    private Path outPath;
    private String inPath;
    private String schemaPath;

    static class JsonMapper extends Mapper<LongWritable, Text, Text, AvroValue<GenericData.Record>> {

        private AvroValue<GenericData.Record> avroOutVal;
        private Text keyText;
        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            schema = new Schema.Parser().parse(context.getConfiguration().get("schema"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            JsonDecoder json = DecoderFactory.get().jsonDecoder(schema, value.toString());
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            GenericData.Record datum = reader.read(null,json);

            keyText = new Text(Integer.toString(datum.hashCode()));
            avroOutVal = new AvroValue<>(datum);

            context.write(keyText,avroOutVal);
        }
    }


    public static class ReduceIt extends Reducer<Text, AvroValue<GenericData.Record>, AvroKey<GenericData.Record>, NullWritable> {

        private AvroKey<GenericData.Record> keyOut;
        private final NullWritable nw = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<AvroValue<GenericData.Record>> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (AvroValue<GenericData.Record> val : values) {
                keyOut = new AvroKey<>(val.datum());
                context.write(keyOut, nw);
            }
        }
    }

    public static Schema getSchema(FileSystem fs, String schemaFile) {
        Schema fileSchema = null;
        try {
            Path schemaPath = new Path(schemaFile);
            InputStream is = fs.open(schemaPath).getWrappedStream();
            System.out.println("Schema read from " + schemaPath.getName());
            fileSchema = new Schema.Parser().parse(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSchema;
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            System.out.println("arg:" + args[i]);
            switch (i - (args.length - 3)) {
                case 0:
                    inPath = args[i];
                    break;
                case 1:
                    outPath = new Path(args[i]);
                    break;
                case 2:
                    schemaPath = args[i];
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        parseArgs(args);

        Configuration config = this.getConf();
        config.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        FileSystem fs = FileSystem.get(config);
        Schema schema = getSchema(fs, schemaPath);
        config.set("schema", schema.toString());
        Job job = Job.getInstance(config);
        job.setJobName("StevesJSONtoAvroConvertor");
        job.setJarByClass(JsonToAvro.class);

        //MapReduceUtils.addJars(fs, job, "/user/sgar241/AvroAppender/lib");
        FileInputFormat.addInputPaths(job, inPath);
        FileInputFormat.setInputDirRecursive(job, true);

        FileOutputFormat.setOutputPath(job, outPath);
        job.setMapperClass(JsonMapper.class);
        job.setReducerClass(ReduceIt.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);
        job.setOutputKeyClass(AvroKey.class);



        AvroJob.setMapOutputValueSchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);


        job.setNumReduceTasks(1);

        //Check to see if the output path already exists and if it does
        //delete it.
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JsonToAvro(), args);
        System.exit(res);
    }
}
