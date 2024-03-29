import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.ArrayList;

public class PageRank extends Configured implements Tool {
    public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        double hangRank = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double pageRank;
            String[] adjVertex = value.toString().split("\t");
            int id = Integer.valueOf(adjVertex[0]);

            String toNodes;
            if(adjVertex.length == 2) {
                pageRank = (double) 1 / Config.AllNums;
                toNodes = adjVertex[1];
            } else {
                pageRank = Double.valueOf(adjVertex[1]);
                toNodes = adjVertex[2];
            }

            ArrayList<Integer> toNodesList = new ArrayList<>();
            if(!toNodes.equals(Config.HANGING_VERTEX)) {
                for (String str: toNodes.split(" ")) {
                    if (str.isEmpty()) {
                        continue;
                    }
                    toNodesList.add(Integer.valueOf(str));
                }
                context.write(new IntWritable(id), new Text( "ToNodes:\t" + toNodes));
            }

            if(toNodesList.size() != 0) {
                double nextPageRank = pageRank / toNodesList.size();
                for(Integer nextId : toNodesList) {
                    context.write(new IntWritable(nextId), new Text(String.valueOf(nextPageRank)));
                }
            } else {
                hangRank = hangRank + pageRank;
            }
        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException{
            Path tmp = new Path(Config.HANG_RANK_PATH + context.getTaskAttemptID().getTaskID().toString());
            FileSystem fs = tmp.getFileSystem(context.getConfiguration());

            FSDataOutputStream file = fs.create(tmp);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(file));
            writer.write(String.valueOf(hangRank));
            writer.close();
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        double rankHang = 0;

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            System.out.println("Start reducer");
            Path graph = new Path(Config.URLS_IDX_PATH);
            FileSystem fs = graph.getFileSystem(context.getConfiguration());

            Path hangRankPath = new Path(Config.HANG_RANK_PATH);
            RemoteIterator<LocatedFileStatus> i = fs.listFiles(hangRankPath, false);
            while(i.hasNext()){
                LocatedFileStatus fileStatus = i.next();
                FSDataInputStream hangRankFile = fs.open(fileStatus.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(hangRankFile));
                double tmpRankHang = Double.valueOf(reader.readLine());
                System.out.println(tmpRankHang);
                rankHang = rankHang + tmpRankHang;
            }
            rankHang = rankHang / Config.AllNums;
            System.out.println("End reducer");
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pageRank = (1.0 - Config.D) * (1.0 / Config.AllNums) + Config.D * rankHang;

            String nextNodes = "";
            for (Text val: values) {
                if (val.toString().lastIndexOf("ToNodes:") == -1) {
                    pageRank += Config.D * Double.valueOf(val.toString());
                } else {
                    String[] splits = val.toString().split("\t");
                    nextNodes = splits[1];
                }
            }
            if (nextNodes.length() == 0) {
                nextNodes = Config.HANGING_VERTEX;
            }
            context.write(key, new Text(pageRank + "\t" + nextNodes));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        for (int i = 0; i < Config.ITERATIONS; i++) {
            System.out.println("Job number: " + String.valueOf(i));
            Job job = Job.getInstance(getConf());
            job.setJarByClass(PageRank.class);
            job.setJobName("PageRank");

            job.setInputFormatClass(TextInputFormat.class);
            if(i == 0) {
                FileInputFormat.addInputPath(job, new Path(strings[0]));
            } else {
                FileInputFormat.addInputPath(job, new Path(strings[1] + (i-1) + "/part-*"));
            }

            FileSystem fs = FileSystem.get(getConf());
            if(fs.exists(new Path(strings[1] + i))) {
                fs.delete(new Path(strings[1] + i), true);
            }

            if(fs.exists(new Path(Config.HANG_RANK_PATH))){
                System.out.println("True");
                fs.delete(new Path(Config.HANG_RANK_PATH), true);
                fs.mkdirs(new Path(Config.HANG_RANK_PATH));
            } else{
                fs.mkdirs(new Path(Config.HANG_RANK_PATH));
            }

            FileOutputFormat.setOutputPath(job, new Path(strings[1] + i));

            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);

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
        int exit = ToolRunner.run(new PageRank(), args);
        System.exit(exit);
    }
}
