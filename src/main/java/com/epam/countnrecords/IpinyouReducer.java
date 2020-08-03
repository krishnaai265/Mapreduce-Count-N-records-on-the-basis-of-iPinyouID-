package com.epam.countnrecords;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpinyouReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private Map<String, Integer> hashMap= new HashMap<String, Integer>();

    public Map<String , Integer > sortMap (Map<String,Integer> unsortMap){
        Map<String ,Integer> hashmap = new LinkedHashMap<String,Integer>();
        int count=0;
        List<Map.Entry<String,Integer>> list = new
                LinkedList<Map.Entry<String,Integer>>(unsortMap.entrySet());
        //Sorting the list we created from unsorted Map
        Collections.sort(list , new Comparator<Map.Entry<String,Integer>>(){

            public int compare (Map.Entry<String , Integer> o1 , Map.Entry<String , Integer> o2 ){
                //sorting in descending order
                return o2.getValue().compareTo(o1.getValue());
            }


        });

        for(Map.Entry<String, Integer> entry : list){
            // only writing top 3 in the sorted map
            if(count>100)
                break;
            hashmap.put(entry.getKey(),entry.getValue());
            ++count;
        }
        return hashmap ;
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        hashMap.put(key.toString(), sum);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Map<String , Integer>  sortedMap = new HashMap<String , Integer>();
        sortedMap = sortMap(hashMap);
        for (Map.Entry<String,Integer> entry : sortedMap.entrySet()){
            context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
        }
    }
}
