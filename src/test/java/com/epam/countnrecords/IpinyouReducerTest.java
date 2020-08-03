package com.epam.countnrecords;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IpinyouReducerTest {
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        IpinyouReducer mapper = new IpinyouReducer();
        reduceDriver = ReduceDriver.newReduceDriver(mapper);
    }

    @Test
    public void testIpinyouReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        reduceDriver.withInput(new Text("35"), values);
        reduceDriver.withInput(new Text("36"), values1);
        reduceDriver.withOutput(new Text("35"), new IntWritable(2));
        reduceDriver.withOutput(new Text("36"), new IntWritable(1));
        reduceDriver.runTest();
    }
}
