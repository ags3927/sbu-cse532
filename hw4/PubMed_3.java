package org.ags;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class PubMed_3 extends Configured implements Tool {

    public static class CustomMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text journal = new Text();
        private final static IntWritable iW = new IntWritable(1);
        private int yearOfInterest;

        @Override
        public void setup(Context ctx) {
            Configuration config = ctx.getConfiguration();
            yearOfInterest = Integer.parseInt(config.get("year_of_interest"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String result = value.toString();
            String[] splitStrings = result.split("\t");
            int year = Integer.parseInt(splitStrings[6]);
            if (year != yearOfInterest) {
                return;
            }
            journal.set(splitStrings[7]);
            context.write(journal, iW);
        }
    }

    public static class CustomReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Format: PubMed_3 INPUT_PATH YEAR_OF_INTEREST OUTPUT_PATH");
            return -1;
        }

        int yearOfInterest = Integer.parseInt(args[1]);
        Job customJob = Job.getInstance(getConf());

        customJob.setJarByClass(PubMed_3.class);
        customJob.setJobName("PubMed_3");

        customJob.setMapperClass(CustomMapper.class);
        customJob.setReducerClass(CustomReducer.class);

        customJob.setOutputKeyClass(Text.class);
        customJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(customJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(customJob, new Path(args[2]));

        customJob.getConfiguration().setInt("year_of_interest", yearOfInterest);

        boolean success = customJob.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Format: PubMed_3 INPUT_PATH YEAR_OF_INTEREST OUTPUT_PATH");
            return;
        }

        int yearOfInterest = Integer.parseInt(args[1]);
        Configuration config = new Configuration();
        
        Job customJob = Job.getInstance(conf, "PubMed_3");
        customJob.setJarByClass(PubMed_3.class);

        customJob.setMapperClass(CustomMapper.class);
        customJob.setReducerClass(CustomReducer.class);

        customJob.setOutputKeyClass(Text.class);
        customJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(customJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(customJob, new Path(args[2]));

        customJob.getConfiguration().setInt("year_of_interest", yearOfInterest);

        customJob.waitForCompletion(true);

    }
}
