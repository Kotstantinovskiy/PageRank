import org.apache.hadoop.conf.Configured;
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
import java.util.ArrayList;

public class HITS extends Configured implements Tool {

    public static class HITSAuthMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double hub = 0;
            String[] adjVertex = value.toString().split("\t");
            int id = Integer.valueOf(adjVertex[0]);

            String toNodes;
            if(adjVertex.length == 2) {
                hub = 1;
                toNodes = adjVertex[1];
            } else {
                hub = Double.valueOf(adjVertex[2]);
                toNodes = adjVertex[1];
            }

            ArrayList<Integer> toNodesList = new ArrayList<>();
            if(!toNodes.equals(Config.HANGING_VERTEX)) {
                for (String node: toNodes.split(" ")) {
                    if (node.isEmpty()) {
                        continue;
                    }
                    toNodesList.add(Integer.parseInt(node));
                }
            }

            // context.write(new IntWritable(id), new Text("ToNodes\t" + toNodes));
            if(toNodesList.size() != 0) {
                for(Integer nextId : toNodesList) {
                    context.write(new IntWritable(nextId), new Text( String.valueOf(id) + "\t" + String.valueOf(hub)));
                }
            }
        }
    }

    public static class HITSAuthReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double auth = 0;

            StringBuilder nextNodes = new StringBuilder();
            for (Text val: values) {
                String[] id_hub = val.toString().split("\t");
                auth += Double.valueOf(id_hub[1]);
                nextNodes.append(id_hub[0]).append(" ");
            }

            if (nextNodes.length() != 0) {
                context.write(key, new Text(nextNodes + "\t" + auth));
            }
        }
    }

    public static class HITSHubMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double auth = 0;
            String[] adjVertex = value.toString().split("\t");
            int id = Integer.valueOf(adjVertex[0]);

            String fromNodes;
            if(adjVertex.length == 2) {
                auth = 1;
                fromNodes = adjVertex[1];
            } else {
                auth = Double.valueOf(adjVertex[2]);
                fromNodes = adjVertex[1];
            }

            ArrayList<Integer> fromNodesList = new ArrayList<>();
            if(!fromNodes.equals(Config.HANGING_VERTEX)) {
                for (String node: fromNodes.split(" ")) {
                    if (node.isEmpty()) {
                        continue;
                    }
                    fromNodesList.add(Integer.parseInt(node));
                }
            }

            // context.write(new IntWritable(id), new Text("ToNodes\t" + toNodes));
            if(fromNodesList.size() != 0) {
                for(Integer pastId : fromNodesList) {
                    context.write(new IntWritable(pastId), new Text( String.valueOf(id) + "\t" + String.valueOf(auth)));
                }
            }
        }
    }

    public static class HITSHubReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double hub = 0;

            StringBuilder pastNodes = new StringBuilder();
            for (Text val: values) {
                String[] id_auth = val.toString().split("\t");
                hub += Double.valueOf(id_auth[1]);
                pastNodes.append(id_auth[0]).append(" ");
            }

            if (pastNodes.length() != 0) {
                context.write(key, new Text(pastNodes + "\t" + hub));
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        for (int i = 0; i < Config.ITERATIONS; i++) {

            Job job = Job.getInstance(getConf());
            job.setJarByClass(HITS.class);
            job.setJobName("main.java.HITS");

            job.setInputFormatClass(TextInputFormat.class);
            if(i == 0) {
                FileInputFormat.addInputPath(job, new Path(strings[0] + "_auth"));
            } else if(i == 1) {
                FileInputFormat.addInputPath(job, new Path(strings[0] + "_hub"));
            } else {
                if(i%2 == 0) {
                    FileInputFormat.addInputPath(job, new Path(strings[1] + "_auth_" + (i-1) + "/part-*"));
                } else {
                    FileInputFormat.addInputPath(job, new Path(strings[1] + "_hub_" + (i-1) + "/part-*"));
                }
            }

            if(i%2 == 0) {
                FileOutputFormat.setOutputPath(job, new Path(strings[1] + "_auth_" + i));
            } else {
                FileOutputFormat.setOutputPath(job, new Path(strings[1] + "_hub_" + i));
            }

            if(i%2 == 0) {
                job.setMapperClass(HITSAuthMapper.class);
                job.setReducerClass(HITSAuthReducer.class);
            } else {
                job.setMapperClass(HITSHubMapper.class);
                job.setReducerClass(HITSHubReducer.class);
            }

            job.setNumReduceTasks(Config.NUM_REDICERS);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            if (!job.waitForCompletion(true)) {
                return 1;
            }
        }
        return 0;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new HITS(), args);
        System.exit(ret);
    }
}
