import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class BuildGraphMR extends Configured implements Tool {

    public static class BuildGraphMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private String toNormal(String url){
            url = url.replace(" ", "");
            url = url.replace("\t", "");
            url = url.replace("\n", "");

            if(url.startsWith("www.")){
                url = url.replace("www.", "");
            }

            url = url.replace("https://", "http://");

            if (url.charAt(url.length()-1) == '/') {
                url = url.substring(0, url.length()-1);
            }

            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                return null;
            }

            uri = uri.normalize();

            if(uri.getHost() == null) {
                return null;
            } else if(uri.getHost().lastIndexOf(Config.LENTA) == -1) {
                return null;
            }

            return uri.toString().toLowerCase();
        }

        private String toNormal(String host_url, String url){

            URI host_uri;
            try {
                host_uri = new URI(host_url);
            } catch (URISyntaxException e) {
                return null;
            }

            url = url.replace(" ", "");
            url = url.replace("\t", "");
            url = url.replace("\n", "");

            if(url.startsWith("www.")){
                url = url.replace("www.", "");
            }

            url = url.replace("https://", "http://");

            if (url.startsWith("//")) {
                url = "http:" + url;
            } else if (url.startsWith("/")) {
                url = "http://" + host_uri.getHost() + url;
            } else if (!url.startsWith("http:")) {
                url = host_url + "/" + url;
            }

            if (url.charAt(url.length()-1) == '/') {
                url = url.substring(0, url.length()-1);
            }

            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                return null;
            }

            uri = uri.normalize();

            if(uri.getHost() == null) {
                return null;
            }

            return uri.toString().toLowerCase();
        }

        private HashSet<String> getAllLinks(String host_url, String doc) {
            HashSet<String> links = new HashSet<>();
            Matcher m = Config.HREF_PATTERN.matcher(doc);
            while (m.find()) {
                if (m.group().lastIndexOf("mailto:") != -1) {
                    continue;
                }

                String normal_url = toNormal(host_url, m.group(2));
                if (normal_url != null) {
                    links.add(normal_url);
                }
            }
            return links;
        }

        private HashMap<Integer, String> docid_url = new HashMap<>();
        private HashMap<String, Integer> url_id = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            System.out.println("Start mapper");
            Path urls = new Path(Config.URLS_PATH);
            FileSystem fs = urls.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(urls);
            BufferedReader reader = new BufferedReader(new InputStreamReader(file));
            String line = reader.readLine();
            while (line != null && !line.equals("")) {
                String[] id_link = line.split("\t");
                int id = Integer.valueOf(id_link[0]);
                String link = toNormal(id_link[1]);
                if (link != null) {
                    docid_url.put(id, link);
                }
                line = reader.readLine();
            }
            reader.close();

            Path doc_id = new Path(Config.URLS_IDX_PATH);
            fs = doc_id.getFileSystem(context.getConfiguration());
            file = fs.open(doc_id);
            reader = new BufferedReader(new InputStreamReader(file));
            line = reader.readLine();
            while (line != null && !line.equals("")) {
                String[] id_link = line.split("\t");
                int id = Integer.valueOf(id_link[0]);
                String link = toNormal(id_link[1]);
                if (link != null) {
                    url_id.put(link, id);
                }
                line = reader.readLine();
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] documentsParts = value.toString().split("\t");
            int documentID = Integer.valueOf(documentsParts[0]);
            Inflater documentParser = new Inflater();
            documentParser.setInput(Base64.decodeBase64(documentsParts[1].getBytes()));
            documentParser.finished();
            byte[] buffer = new byte[2048];
            String document = "";
            while (true) {
                try {
                    int decompressedLen = documentParser.inflate(buffer);
                    if (decompressedLen > 0) {
                        document += new String(buffer, 0, decompressedLen);
                    } else {
                        break;
                    }
                } catch (DataFormatException exc) {
                    //LOG.warn("Ошибка декодировки! ID = " + documentID);
                    break;
                }
            }

            HashSet<String> links = getAllLinks(docid_url.get(documentID), document);

            int documentLinkID = url_id.get(docid_url.get(documentID));
            context.write(new IntWritable(documentLinkID), new IntWritable(-1));
            for (String link: links) {
                Integer currentLinkId = url_id.get(link);
                if (currentLinkId == null) {
                    continue;
                }
                //System.out.println("1");
                context.write(new IntWritable(documentLinkID), new IntWritable(currentLinkId));
                context.write(new IntWritable(currentLinkId), new IntWritable(-1));
                //System.out.println("2");
            }
        }
    }

    public static class BuildGraphReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Start reducer");
            StringBuilder result = new StringBuilder();
            for (IntWritable val: values) {
                if(val.get() != -1) {
                    result.append(val.get()).append(" ");
                }
            }

            if(result.toString().equals("")){
                context.write(new Text(key.toString()), new Text(Config.HANGING_VERTEX));
            } else {
                context.write(new Text(key.toString()), new Text(result.toString().substring(0, result.toString().length() - 1)));
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(BuildGraphMR.class);

        FileSystem fs = FileSystem.get(getConf());
        if(fs.exists(new Path(strings[1]))) {
            fs.delete(new Path(strings[1]), true);
        }

        job.setJobName("BuildGraph");

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(BuildGraphMR.BuildGraphMapper.class);
        job.setReducerClass(BuildGraphMR.BuildGraphReducer.class);

        job.setNumReduceTasks(Config.NUM_REDICERS);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new BuildGraphMR(), args);
        System.exit(ret);
    }
}