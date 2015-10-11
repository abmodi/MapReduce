package com.wikiGeolocation;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WikiGeolocationReducer
extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context)
					throws IOException, InterruptedException {
		String output = "";
		for (Text value : values) {
			output  += value.toString() + " ,";
		}
		context.write(key, new Text(output));
	}
}