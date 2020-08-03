package com.epam.countnrecords;

import com.epam.countnrecords.IpinyouMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class IpinyouMapperTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        IpinyouMapper mapper = new IpinyouMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testIpinyouMapper() throws IOException {
        final String testString = "A A 35";
        final String testString2 = "A B 36";
        final String testString3 = "B C 35";
        mapDriver.withInput(new LongWritable(), new Text(testString));
        mapDriver.withInput(new LongWritable(), new Text(testString2));
        mapDriver.withInput(new LongWritable(), new Text(testString3));
        mapDriver.withOutput(new Text("35"), new IntWritable(1));
        mapDriver.withOutput(new Text("36"), new IntWritable(1));
        mapDriver.withOutput(new Text("35"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testnullValue() throws IOException {
        final String testString = "A A 35";
        final String testString2 = "A B null";
        final String testString3 = "B C 35";
        mapDriver.withInput(new LongWritable(), new Text(testString));
        mapDriver.withInput(new LongWritable(), new Text(testString2));
        mapDriver.withInput(new LongWritable(), new Text(testString3));
        mapDriver.withOutput(new Text("35"), new IntWritable(1));
        mapDriver.withOutput(new Text("35"), new IntWritable(1));
        mapDriver.runTest();
    }
}
