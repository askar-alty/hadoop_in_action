import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author Askar Shabykov
 * @since 02.06.17
 */
public class DataJoinTask extends Configured implements Tool {

    public static class DataJoinMapper extends DataJoinMapperBase {

        public Text generateInputTag(String inputFile) {
            return new Text(inputFile);
        }

        public Text generateGroupKey(TaggedMapOutput aRecoder) {
            String line = ((Text) aRecoder.getData()).toString();
            String[] fields = line.split(",");
            String groupKey = fields[1];
            return new Text(groupKey);
        }

        public TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable retv = new TaggedWritable((Text)value);
            retv.setTag(this.inputTag);
            return retv;
        }
    }

    public static class DataJoinReducer extends DataJoinReducerBase {
        public TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2) return null;
            String joinStr = "";
            for (int i = 0; i < values.length; i++) {
                if (i > 0) joinStr += ",";
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                String[] tokens = line.split(",", 2);
                joinStr += tokens[1];
            }
            return new TaggedWritable(new Text(joinStr), (Text) tags[0]);
        }

    }

    public static class TaggedWritable extends TaggedMapOutput {

        private Writable data;
        private Text tag;

        TaggedWritable(Writable data, Text tag) {
            this.data = data;
            this.tag = tag;

        }

        TaggedWritable(Writable data) {
            this.data = data;
            this.tag = new Text("");
        }

        public Writable getData() {
            return data;
        }

        public Text getTag() {
            return tag;
        }

        public void setData(Writable data) {
            this.data = data;
        }

        public void setTag(Text tag) {
            this.tag = tag;
        }

        public void write(DataOutput dataOutput) throws IOException {
            this.tag.write(dataOutput);
            this.data.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.tag.readFields(dataInput);
            this.data.readFields(dataInput);
        }


    }


    public int run(String[] args) throws IOException {
        Configuration configuration = getConf();
        JobConf job = new JobConf(configuration, this.getClass());

        Path in_put1 = new Path("/user/askar/join/customer.csv");
        Path in_put2 = new Path("/user/askar/join/orders.csv");
        Path out_put = new Path("/user/askar/output");

        FileInputFormat.addInputPath(job, in_put1);
        FileInputFormat.addInputPath(job, in_put2);
        FileOutputFormat.setOutputPath(job, out_put);

        job.setMapperClass(DataJoinMapper.class); // mapper class
        job.setReducerClass(DataJoinReducer.class); // reducer class
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        job.set("mapred.textoutputformat.separator", ",");
        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new DataJoinTask(), args);
        System.exit(exitCode);
    }
}
