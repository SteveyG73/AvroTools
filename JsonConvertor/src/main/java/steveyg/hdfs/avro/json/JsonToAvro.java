package steveyg.hdfs.avro.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JsonToAvro extends Configured implements Tool{


    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JsonToAvro(), args);
        System.exit(res);
    }
}
