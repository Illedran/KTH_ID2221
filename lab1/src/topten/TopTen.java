package topten;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Integer.parseInt;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> userInfo = transformXmlToMap(value.toString());
            // Check that the XML parsing succeeded and Id is valid, as specified in assignment
            if (!userInfo.isEmpty() && userInfo.get("Id") != null) {
                repToRecordMap.put(parseInt(userInfo.get("Reputation")), new Text(value));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for (int i = 0; i < 10; i++) {
                // TreeMap sorts by increasing key, hence last key (rep) is highest entry
                Map.Entry<Integer, Text> entry = repToRecordMap.pollLastEntry();
                context.write(NullWritable.get(), entry.getValue());
            }
        }
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> userInfo = transformXmlToMap(value.toString());
                // no need to check for validity as rows made it through the mapper
                repToRecordMap.put(parseInt(userInfo.get("Reputation")), new Text(value));
            }
            for (int i = 0; i < 10; i++) {
                Map.Entry<Integer, Text> entry = repToRecordMap.pollLastEntry();
                Map<String, String> userInfo = transformXmlToMap(entry.getValue().toString());

                Put insHBase = new Put(Integer.toString(i).getBytes());

                // insert to hbase
                // Writing data as strings instead of bytes so that it is human readable in the hbase shell
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(entry.getKey().toString()));
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(userInfo.get("Id")));

                context.write(NullWritable.get(), insHBase);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "topten");
        job.setJarByClass(TopTen.class);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        // define input files
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // define output table
        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
