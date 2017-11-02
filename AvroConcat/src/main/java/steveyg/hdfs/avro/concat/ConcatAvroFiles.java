package steveyg.hdfs.avro.concat;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;

public class ConcatAvroFiles extends Configured implements Tool {

    public static class MapIt extends Mapper<AvroKey<GenericData.Record>, NullWritable, Text, AvroValue<GenericData.Record>> {

        private Text fname = new Text();
        private AvroValue<GenericData.Record> valout;

        //Mapper will read an avro file
        @Override
        public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            FileSplit fSplit = (FileSplit) context.getInputSplit();
            String fileName = fSplit.getPath().getName();
            fname.set(fileName);

            valout = new AvroValue<>(key.datum());
            context.write(fname, valout);
        }

    }

    public static class ReduceIt extends Reducer<Text, AvroValue<GenericData.Record>, AvroKey<GenericData.Record>, NullWritable> {

        private AvroKey<GenericData.Record> keyout;
        private final NullWritable nw = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<AvroValue<GenericData.Record>> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (AvroValue<GenericData.Record> val : values) {
                keyout = new AvroKey<>(val.datum());
                context.write(keyout, nw);
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

    public static void addJars(FileSystem fs, Job job, String jarPath) throws IOException {
        FileStatus[] status = fs.listStatus(new Path(jarPath));

        if (status != null) {
            System.out.println("Found some files...");
            for (FileStatus statu : status)
                if (!statu.isDirectory()) {
                    Path hdfsJarPath = new Path(statu.getPath().toUri().getPath());
                    System.out.println("Adding " + hdfsJarPath.getName());
                    job.addArchiveToClassPath(hdfsJarPath);
                }
        } else {
            System.out.println("Didn't find any jar files...");
        }
    }

    /**
     *
     */

    public static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.out.println("classpath BEGIN");
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
        System.out.println("classpath END");
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConcatAvroFiles(), args);
        System.exit(res);
    }

    /**
     * @param args Usage: ConcatAvroFiles <input path> <output path> <schema file>
     * @return 0 or -1 success/fail
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ConcatAvroFiles <input path> <output path> <schema file>");
            return -1;
        }
        String inPath = null;
        Path outPath = null;
        String schemaPath = null;
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

        Configuration conf = this.getConf();
        //This is key - ensures that your uploaded JAR files will appear in the
        //classpath before all the default ones.
        //This allows you to use a newer version of Avro (for instance)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf);
        job.setJobName("StevesAvroThing");
        FileSystem fs = FileSystem.get(conf);
        addJars(fs, job, "/user/sgar241/AvroAppender/lib");
        FileInputFormat.addInputPaths(job, inPath);
        //This allows you to pass in a directory to the inPath parameter
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        //Set the map and reduce classes
        job.setMapperClass(MapIt.class);
        job.setReducerClass(ReduceIt.class);

        //Can't honestly remember what this does!
        job.setJarByClass(ConcatAvroFiles.class);
        printClassPath();

        //Input format will be an Avro Key
        job.setInputFormatClass(AvroKeyInputFormat.class);
        //Output format also an Avro Key
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        //Set the class for the output mapper key
        job.setMapOutputKeyClass(Text.class);
        //Set the class for the output mapper value
        job.setMapOutputValueClass(AvroValue.class);

        //Go get the schema file
        Schema schema = getSchema(fs, schemaPath);

        //Set the  schema for the input keys
        AvroJob.setInputKeySchema(job, schema);
        //Set the schema for the output values from the mapper
        AvroJob.setMapOutputValueSchema(job, schema);
        //Set the schema for the output of the job
        AvroJob.setOutputKeySchema(job, schema);
        //Set the class for the output of the key from the job
        job.setOutputKeyClass(AvroKey.class);
        //Set the class for the output of the value from the job
        job.setOutputValueClass(NullWritable.class);

        //Check to see if the output path already exists and if it does
        //delete it.
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        //Wait for the job to complete (i.e. don't just fire and forget)
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
