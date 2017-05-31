import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;


/**
 * @author Askar Shabykovt
 * @since 30.05.17
 */


public class InvertingData extends Configured implements Tool {

    public static class InvertingMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            output.collect(value, key);
        }
    }

    public static class InvertingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String csv = "";
            while (values.hasNext()) {
                if (csv.length() > 0) csv += ",";
                csv += values.next().toString();
            }
            output.collect(key, new Text(csv));
        }
    }

    public int run(String[] args) {
        Configuration configuration = this.getConf();
        JobConf job = new JobConf(configuration, InvertingData.class);
        Path in_put = new Path(args[0]);
        Path out_put = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in_put);
        FileOutputFormat.setOutputPath(job, out_put);

        job.setJobName("Invertor");
        job.setMapperClass(InvertingMapper.class);
        job.setReducerClass(InvertingReducer.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.set("key.value.separator.in.input.line", "");

        try {
            JobClient.runJob(job);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InvertingData(), args);
        System.exit(res);

    }
}
