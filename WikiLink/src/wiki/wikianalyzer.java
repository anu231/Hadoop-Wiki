package wiki;

        
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;        
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.logging.Log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;



public class wikianalyzer extends Configured implements Tool{
        
 
 
 public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        Text k2 = new Text();
        k2.set("Anu");
        k2.append(key.getBytes(), 0, key.getBytes().length);
        context.write(k2, new IntWritable(sum));
    }
 }
 
 
 public static class WikiPageInputFormat extends TextInputFormat{
	 	//public PageInputFormat(){}
	 	//public PageInputFormat(Object... o){}
	    @Override
	    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
	        return new WikiRecordReader();
	    }
	}

public class wikiArray extends ArrayWritable{
	public wikiArray(Class<? extends Writable> valueClass, Writable[] values) {
	    super(valueClass, values);
	}
	public wikiArray(Class<? extends Writable> valueClass) {
	    super(valueClass);
	}
	@Override
	public Text[] get() {
	    return (Text[]) super.get();
	}
	@Override
	public String toString(){
		return Arrays.toString(get());
	}
}

public static void main(String[] args) throws Exception {
	 
	 int res = ToolRunner.run(new wikianalyzer(), args);
     System.exit(res);
	/* 
	System.out.println("Anu:Job Starting");
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "WikiLinks");
    job.setJarByClass(wikianalyzer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    //job.setInputFormatClass(TextInputFormat.class);
    job.setInputFormatClass(WikiPageInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    System.out.println("File Input :"+args[0]);
    System.out.println("File Output :"+args[1]);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
    */
 }
@Override
public int run(String[] args) throws Exception {
	// TODO Auto-generated method stub
	System.out.println("Anu:Job Starting");
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "WikiLinks");
    job.setJarByClass(wikianalyzer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    //job.setInputFormatClass(TextInputFormat.class);
    job.setInputFormatClass(WikiPageInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    System.out.println("File Input :"+args[0]);
    System.out.println("File Output :"+args[1]);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    return job.waitForCompletion(true) ? 0:1;
}
}