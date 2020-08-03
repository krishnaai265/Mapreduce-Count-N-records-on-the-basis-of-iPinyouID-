package com.epam.countnrecords;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class IpinyouTest {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        IpinyouMapper mapper = new IpinyouMapper();
        IpinyouReducer reducer = new IpinyouReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testIpinyouMapper() throws IOException {
        final String testString = "A A 35";
        final String testString2 = "A B 36";
        final String testString3 = "B C 35";
        mapReduceDriver.withInput(new LongWritable(), new Text(testString));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString3));
        mapReduceDriver.withOutput(new Text("35"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("36"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

    @Test
    public void testnullValue() throws IOException {
        final String testString = "A A 35";
        final String testString2 = "A B null";
        final String testString3 = "B C 35";
        mapReduceDriver.withInput(new LongWritable(), new Text(testString));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString3));
        mapReduceDriver.withOutput(new Text("35"), new IntWritable(2));
        mapReduceDriver.runTest();
    }
}
