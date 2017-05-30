/**
 * Created by real7 on 26.05.17.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static public void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class); // Mapper
        job.setCombinerClass(IntSumReducer.class); // Combiner
        job.setReducerClass(IntSumReducer.class); // Reducer

        job.setOutputKeyClass(Text.class); // Тип ключа на выходе
        job.setOutputValueClass(IntWritable.class); // Тип значения на выходе

        FileInputFormat.addInputPath(job, new Path(args[0])); // Путь к входным файлам
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Путь к выходному файлу

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
