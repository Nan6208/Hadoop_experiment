package com.bigdata.invertedindex;

import java.net.URI;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outVal = new Text("1");
    private Set<String> stopWords = new HashSet<>();
    
    @Override
    protected void setup(Context context) throws IOException {
   	 BufferedReader br = new BufferedReader(
  	          new InputStreamReader(new FileInputStream("stopwords.txt")));
 	   String line;
 	   while ((line = br.readLine()) != null) {
 	       String w = line.trim().toLowerCase();
 	       if (!w.isEmpty()) stopWords.add(w);
 	  }
  	  br.close();
    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();

        if (fileName.equals("stopwords.txt")) return;

        StringTokenizer tokens = new StringTokenizer(value.toString());
        while (tokens.hasMoreTokens()) {
            String word = tokens.nextToken()
                    .replaceAll("[^a-zA-Z]", "")
                    .toLowerCase();
            if (word.isEmpty() || stopWords.contains(word)) continue;

            outKey.set(word + ":" + fileName);
            context.write(outKey, outVal);
        }
    }
}
