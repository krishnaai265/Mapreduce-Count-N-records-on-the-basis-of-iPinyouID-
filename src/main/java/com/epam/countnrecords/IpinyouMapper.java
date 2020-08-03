package com.epam.countnrecords;

import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IpinyouMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Splitting the line on spaces
        String[] stringArr = value.toString().split("\\s+");
        String ipinyouid = stringArr[2];
        if(!ipinyouid.equals("null")) {
            context.write(new Text(ipinyouid), new IntWritable(1));
        }
    }
}
