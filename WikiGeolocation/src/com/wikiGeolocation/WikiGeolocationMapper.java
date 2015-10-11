package com.wikiGeolocation;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiGeolocationMapper extends
Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String dataRow = value.toString();
		
        StringTokenizer dataTokenizer = new StringTokenizer(dataRow, "\t");
        String articleName = dataTokenizer.nextToken();
        String pointType = dataTokenizer.nextToken();
        String geoPoint = dataTokenizer.nextToken();
        
        if(pointType.contains("georss/point")) {
	        // now we process the GEO point data.
	        StringTokenizer st = new StringTokenizer(geoPoint, " ");
	        String strLat = st.nextToken();
	        String strLong = st.nextToken();
	        double lat = Double.parseDouble(strLat);
	        double lang = Double.parseDouble(strLong);
	        long roundedLat = Math.round(lat);
	        long roundedLong = Math.round(lang);
	        String locationKey = "(" + String.valueOf(roundedLat) + ","
	                        + String.valueOf(roundedLong) + ")";
	        String locationName = URLDecoder.decode(articleName, "UTF-8");
	        locationName = locationName.replace("_", " ");
	        context.write(new Text(locationKey), new Text(locationName));
        }
	}
}