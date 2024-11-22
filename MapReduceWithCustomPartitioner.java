// Custom partitioner in Java 
// Selected Topics || Spring 2023-2024 Project 

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class LogProcessorMR {



    public static class LogProcessorMapper extends Mapper<LongWritable, Text, Text, Text> {





        public static List<String> monthsList = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
                "Sep", "Oct", "Nov", "Dec");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



            String[] fields = value.toString().split(" ");

            if (fields.length > 3) {
                String ipAddress = fields[0];
                String[] dateFields = fields[3].split("/");
                if (dateFields.length > 1) {
                    String theMonth = dateFields[1];
                    if (monthsList.contains(theMonth))
                        context.write(new Text(ipAddress), new Text(theMonth));
                }
            }
        }
    }








    public static class LogProcessorReducer extends Reducer<Text, Text, Text, IntWritable> {

        IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int numberOfHits = 0;
            for (@SuppressWarnings("unused") Text val : values) {
                numberOfHits++;
            }

            result.set(numberOfHits);
            context.write(key, result);
        }
    }

    public static class LogProcessorMonthPartitioner extends Partitioner<Text, Text> implements Configurable {

        private Configuration configuration;
        HashMap<String, Integer> months = new HashMap<String, Integer>();

        @Override
        public void setConf(Configuration configuration) {
            this.configuration = configuration;
            months.put("Jan", 0);
            months.put("Feb", 1);
            months.put("Mar", 2);
            months.put("Apr", 3);
            months.put("May", 4);
            months.put("Jun", 5);
            months.put("Jul", 6);
            months.put("Aug", 7);
            months.put("Sep", 8);
            months.put("Oct", 9);
            months.put("Nov", 10);
            months.put("Dec", 11);
        }

        @Override
        public Configuration getConf() {
            return configuration;
        }

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            return months.get(value.toString());
        }
    }









    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: LogProcessorMR <input dir> <output dir>\n");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];

        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(new Path(output));
        if (exists) {
            fs.delete(new Path(output), true);
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogProcessorMR.class);

        job.setJobName("Log Processing");

        FileInputFormat.setInputPaths(job, new Path(input));

        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(LogProcessorMapper.class);
        job.setReducerClass(LogProcessorReducer.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(12);

        job.setPartitionerClass(LogProcessorMonthPartitioner.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
