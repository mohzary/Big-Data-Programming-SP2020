package com.lendap.hadoop;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TopCategories {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text category_key = new Text();

        private final static IntWritable data_1 = new IntWritable(1);

        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String data_line = value.toString();
            String str_value[] = data_line.split("\t");



            if(str_value.length > 5){
                category_key.set(str_value[3]);

            }
            context.write(category_key, data_1 );
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable > {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "TopCategories");;

            job.setJarByClass(TopCategories.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            Path out= new Path(args[1]);
            out.getFileSystem(conf).delete(out);

            job.waitForCompletion(true);


        }

}
