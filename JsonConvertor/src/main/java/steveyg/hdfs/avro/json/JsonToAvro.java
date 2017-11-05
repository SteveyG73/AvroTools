package steveyg.hdfs.avro.json;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

    static class JsonMapper extends Mapper<Text, NullWritable, Text, AvroValue<GenericData.Record>> {

        private AvroValue<GenericData.Record> avroOutVal;
        private Text keyText;
        private Schema schema;
        @Override
        protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            schema = new Schema.Parser().parse(context.getConfiguration().get("schema"));
            JsonDecoder json = DecoderFactory.get().jsonDecoder(schema, key.toString());
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            GenericData.Record datum = reader.read(null,json);

            keyText = new Text(Integer.toString(datum.hashCode()));
            avroOutVal = new AvroValue<>(datum);

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

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JsonToAvro(), args);
        System.exit(res);
    }
}
