import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class BuildReverseGraphMR  extends Configured implements Tool {

    public static class BuildReverseGraphMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] documentsParts = value.toString().split("\t");
            String documentID = documentsParts[0];

            if(!documentsParts[1].equals(Config.HANGING_VERTEX)) {
                String[] toDocumentID = documentsParts[1].split(" ");
                for (String toNode : toDocumentID) {
                    context.write(new Text(toNode), new Text(documentID));
                }
                context.write(new Text(documentID), new Text("-1"));
            } else {
                context.write(new Text(documentsParts[0]), new Text("-1"));
            }
        }
    }

    public static class BuildReverseGraphReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            for (Text val : values) {
                if(!val.toString().equals("-1")) {
                    result.append(val.toString()).append(" ");
                }
            }

            if(result.toString().equals("")) {
                context.write(key, new Text(Config.HANGING_VERTEX));
            } else {
                context.write(key, new Text(result.toString().substring(0, result.toString().length() - 1)));
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(BuildReverseGraphMR.class);

        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(new Path(strings[1]))) {
            fs.delete(new Path(strings[1]), true);
        }

        job.setJobName("BuildReverseGraph");

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(BuildReverseGraphMapper.class);
        job.setReducerClass(BuildReverseGraphReducer.class);

        job.setNumReduceTasks(Config.NUM_REDICERS);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new BuildReverseGraphMR(), args);
        System.exit(ret);
    }
}
