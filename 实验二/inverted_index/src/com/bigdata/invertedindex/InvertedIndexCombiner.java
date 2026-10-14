package com.bigdata.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outVal = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String[] parts = key.toString().split(":");
        if (parts.length < 2) return;
        String word = parts[0];
        String fileName = parts[1];

        int sum = 0;
        for (Text val : values) {
            sum += Integer.parseInt(val.toString());
        }

        outKey.set(word);
        outVal.set(fileName + ":" + sum);
        context.write(outKey, outVal);
    }
}
