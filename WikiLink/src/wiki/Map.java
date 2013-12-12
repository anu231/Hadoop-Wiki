package wiki;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable one = new IntWritable(1);
    private Text titleT = new Text();
    private Text refT = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	//String line = value.toString();
    	//System.out.println(line);
    	//System.out.println("-----------------------------------------------------------");
    	WikiParse wp = new WikiParse();
    	String html = wp.parseWikiMarkup(value.toString());
    	System.out.println(html);
    	System.out.println("-----------------------------------------------------------");
    }
 }