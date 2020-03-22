package com.lendap.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class TopRatedVideos {


//Mapper Part
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private Text video_name_key = new Text();
        private FloatWritable rating_value = new FloatWritable();

        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            //to split data in the text file
            String data_line = value.toString();
            String str_value[] = data_line.split("\t");



            if(str_value.length > 7){

                // to get video id
                video_name_key.set(str_value[0]);

                //to get rate value we need to convert it from string value into float
                if(str_value[6].matches("\\d+.+")){
                    float floatFromString= Float.parseFloat(str_value[6]);

                    //to get rate value as float number
                    rating_value.set(floatFromString);
                }

            }
            context.write(video_name_key, rating_value );
        }
    }


    //Reducer Part

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable > {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            //To save the rate values
            float sum = 0;
            // to check how many entries each video has in the dataset so we can compute the average rate later
            int amount_Value = 0;
            for (FloatWritable val : values) {
                amount_Value +=1;
                sum += val.get();
            }

            //To compute the average rate for each video
            sum = sum / amount_Value;
            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "TopRatedVideos");;

        job.setJarByClass(TopRatedVideos.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(TopRatedVideos.Map.class);
        job.setReducerClass(TopRatedVideos.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path out= new Path(args[1]);
        out.getFileSystem(conf).delete(out);

        job.waitForCompletion(true);


    }


}
