package steveyg.hdfs.avro.json;

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
import java.net.URL;
import java.net.URLClassLoader;

public class JsonToAvro extends Configured implements Tool{


    private Path outPath;
    private String inPath;
    private String schemaPath;

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
    public static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.out.println("classpath BEGIN");
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
        System.out.println("classpath END");
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
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JsonToAvro(), args);
        System.exit(res);
    }
}
