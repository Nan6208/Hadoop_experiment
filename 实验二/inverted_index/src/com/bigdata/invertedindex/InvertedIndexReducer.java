package com.bigdata.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text outVal = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        int total = 0;

        for (Text val : values) {
            String item = val.toString();
            sb.append("<").append(item).append(">;");
            String[] parts = item.split(":");
            if (parts.length == 2) {
                total += Integer.parseInt(parts[1]);
            }
        }
        sb.append("<total,").append(total).append(">.");

        outVal.set(sb.toString());
        context.write(key, outVal);
    }
}
