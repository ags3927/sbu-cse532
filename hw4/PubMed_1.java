package org.ags;

import java.io.IOException;
import java.util.*;

import javax.swing.text.AbstractDocument.Content;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PubMed_1 {
    public static class CustomMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable lw = new LongWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            String result = value.toString();
            String[] split_result = result.split("\\t");
            word.set(result[6]);
            ctx.write(word, lw);
        }
    }

    public static class CustomReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sum = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context ctx)
                throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable value : values) {
                total += value.get();
            }
            sum.set(total);
            ctx.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job customJob = Job.getInstance(config, "Word Count Test");
        
        customJob.setJarByClass(PubMed_1.class);
        customJob.setReducerClass(CustomReducer.class);
        customJob.setMapperClass(CustomMapper.class);
        
        customJob.setOutputValueClass(LongWritable.class);
        customJob.setOutputKeyClass(Text.class);
        
        FileInputFormat.addInputPath(customJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(customJob, new Path(args[1]));
        
        System.exit(customJob.waitForCompletion(true) ? 0 : 1);
    }
}