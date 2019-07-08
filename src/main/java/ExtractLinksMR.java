import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class ExtractLinksMR extends Configured implements Tool {

    public static class ExtractLinksMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

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

                String normal_url = toNormal(host_url, m.group(2)); // Проверяет принадлежность lenta.ru, нормализует URL
                if (normal_url != null) {
                    links.add(normal_url);
                }
            }
            return links;
        }

        private HashMap<Integer, String> docid_url = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            System.out.println("Start mapper");
            Path urls = new Path(Config.URLS_PATH);
            FileSystem fs = urls.getFileSystem(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(urls), StandardCharsets.UTF_8));
            String line = reader.readLine();

            while (line != null && !line.equals("")) {
                String[] id_link = line.split("\t");
                int id = Integer.valueOf(id_link[0]);
                String url = toNormal(id_link[1]);
                if (url != null) {
                    docid_url.put(id, url);
                }
                line = reader.readLine();
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] documents_parts = value.toString().split("\t");
            int document_id = Integer.valueOf(documents_parts[0]);

            Inflater document_parser = new Inflater();
            document_parser.setInput(Base64.decodeBase64(documents_parts[1].getBytes()));
            document_parser.finished();
            byte[] buffer = new byte[2048];
            String document = "";

            while (true) {
                try {
                    int decompressed_len = document_parser.inflate(buffer);
                    if (decompressed_len > 0) {
                        document += new String(buffer, 0, decompressed_len);
                    } else {
                        break;
                    }
                } catch (DataFormatException exc) {
                    //LOG.warn("Incorrect document format! ID = " + document_id + "; offset = " + key);
                    break;
                }
            }

            HashSet<String> links = getAllLinks(docid_url.get(document_id), document);

            if(docid_url.get(document_id) == null){
                throw new IOException("No links!" + String.valueOf(document_id));
            } else {
                context.write(new Text(docid_url.get(document_id)), NullWritable.get());
                if(links != null) {
                    for (String link : links) {
                        context.write(new Text(link), NullWritable.get());
                    }
                }
            }
        }
    }

    public static class ExtractLinksReducer extends Reducer<Text, NullWritable, Text, Text> {
        private long url_id = 0;

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Start reducer");
            context.write(new Text(String.valueOf(url_id)), key);
            url_id++;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required!");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("ExtractLinks");

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(ExtractLinksMR.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ExtractLinksMapper.class);
        job.setReducerClass(ExtractLinksReducer.class);

        job.setNumReduceTasks(Config.NUM_REDICERS);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new ExtractLinksMR(), args);
        System.exit(exit);
    }
}
