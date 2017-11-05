package steveyg.hdfs.utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

public final class MapReduceUtils {

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

    private MapReduceUtils() throws Exception {
        throw new Exception();
    }
}
