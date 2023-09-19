package org.ags;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PubMed_2 {

    public static class CustomMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text journalName = new Text();

        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String result = value.toString();
            String[] split_strings = result.split("\\t");
            journalName.set(split_strings[7]);
            ctx.write(journalName, NullWritable.get());
        }
    }

    public static class CustomReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job customJob = Job.getInstance(config, "Unique Journals");

        customJob.setJarByClass(PubMed_2.class);
        customJob.setReducerClass(CustomReducer.class);
        customJob.setMapperClass(CustomMapper.class);
        
        customJob.setOutputValueClass(NullWritable.class);
        customJob.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(customJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(customJob, new Path(args[1]));
        
        customJob.waitForCompletion(true);
    }
}
