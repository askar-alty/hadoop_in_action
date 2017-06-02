import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Askar Shabykov
 * @since 02.06.17
 */
public class ChainMapReduceTask extends Configured implements Tool {

    // Mapper 1
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String country = fields[4];
            if (country.equals("US") || country.equals("RU") || country.equals("FR") || country.equals("GB")) { // фильтр по стране
                context.write(new Text(country), value);
            }
        }
    }


    // Mapper 2
    public static class Mapper2 extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Integer date = Integer.parseInt(fields[1]);
            if (date <= 2000 && date >= 1980) {    /// фильтр по дате
                context.write(key, value);
            }
        }
    }


    // Reducer
    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (Text value : values) {
                String[] fields = value.toString().split(",");
                sum += Double.parseDouble(fields[8]);
                count += 1;
            }
            context.write(key, new Text(String.valueOf(sum) + String.valueOf(count)));
        }
    }


    // Mapper 3
    public static class Mapper3 extends Mapper<Text, Text, LongWritable, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Long sum = Long.parseLong(fields[0]);
            context.write(new LongWritable(sum), new Text(key.toString() + ":" + value.toString()));
        }
    }


    // Mapper 4
    public static class Mapper4 extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "Cain Job");

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path in_put = new Path(args[0]);
        Path out_put = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in_put);
        FileOutputFormat.setOutputPath(job, out_put);

        Configuration mapConf1 = new Configuration(false);
        ChainMapper.addMapper(job, Mapper1.class, LongWritable.class, Text.class, Text.class, Text.class, mapConf1);

        Configuration mapConf2 = new Configuration(false);
        ChainMapper.addMapper(job, Mapper2.class, Text.class, Text.class, Text.class, Text.class, mapConf2);

        Configuration reduceConf1 = new Configuration(false);
        ChainReducer.setReducer(job, Reducer.class, Text.class, Text.class, Text.class, Text.class, reduceConf1);

        Configuration mapConf3 = new Configuration(false);
        ChainReducer.addMapper(job, Mapper3.class, Text.class, Text.class, LongWritable.class, Text.class, mapConf3);

        Configuration mapConf4 = new Configuration(false);
        ChainReducer.addMapper(job, Mapper4.class, LongWritable.class, Text.class, LongWritable.class, Text.class, mapConf4);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ChainMapReduceTask(), args);
        System.exit(exitCode);
    }
}