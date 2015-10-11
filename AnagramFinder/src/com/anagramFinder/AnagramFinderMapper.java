package com.anagramFinder;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramFinderMapper extends
Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			char[] wordChars = word.toCharArray();
	        Arrays.sort(wordChars);
	        String sortedWord = new String(wordChars);
			context.write(new Text(sortedWord), new Text(word));
		}
	}
}