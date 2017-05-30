import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Askar Shabykov
 * @since 30.05.17
 */

public class CitationHistogram extends Configured implements Tool {


    public static class CitationHistogramMapper extends Mapper<Text, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private final static IntWritable citationCount = new IntWritable();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                citationCount.set(Integer.parseInt(value.toString()));
                context.write(citationCount, one);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    public static class CitationHistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
                context.write(key, new IntWritable(count));
            }
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf(); // конфигурации воркера

        configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ","); // line.split(",")


        Job job = Job.getInstance(configuration, "Citation Histogram"); /// создаем воркера
        job.setJarByClass(CitationHistogram.class);

        Path in_put = new Path(args[0]); // путь к входным файлам в hdfs
        Path out_put = new Path(args[1]); // путь к выходному файлу в hdfs

        KeyValueTextInputFormat.addInputPath(job, in_put);
        TextOutputFormat.setOutputPath(job, out_put);

        job.setMapperClass(CitationHistogramMapper.class); // mapper class
        job.setReducerClass(CitationHistogramReducer.class); // reducer class

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class); // тип ключа выходных данных
        job.setOutputValueClass(IntWritable.class); // тип значения выходных данных'

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new CitationHistogram(), args);
        System.exit(exitCode);
    }
}

