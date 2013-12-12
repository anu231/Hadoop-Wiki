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


public class wikianalyzer {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable one = new IntWritable(1);
    private Text titleT = new Text();
    private Text refT = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String line = value.toString();
    	//finding the title for the line
    	int endInd = line.indexOf(":=");
    	String title = line.substring(endInd+2, line.length()-1);
    	
    	
    	String data = line.substring(0,endInd);
    	System.out.println(data);
    	if (data.indexOf("<text xml:space=\"preserve\">")>-1){
    		//System.out.println("text:title : "+title);
        	//System.out.println("map:"+line);
    		
    		WikiParse w = new WikiParse();
    		String htmlText = w.parseWikiMarkup(data);
    		System.out.println(htmlText);
    		/*
    		Pattern p = Pattern.compile("\\[.*?\\]");
    		Matcher match = p.matcher(data);
    		while (match.find()){
    			System.out.println("title : "+title+"\tlink :"+data.substring(match.start(),match.end()));
    			 
    			String reference = data.substring(match.start(),match.end());
    			int ind1 = reference.indexOf("|");
    			if (ind1>=0){
    				reference = reference.substring(0, ind1);
    			}
    			
    			String reference1 = reference.replace("[", "");
    			String reference_final = reference1.replace("]", "");
    			System.out.println("reference : "+reference);
    			titleT.clear();
    			refT.clear();
    			titleT.set(title);
    			refT.set(reference_final);
    			context.write(refT, titleT);
    			*/
    		}
    		//System.out.println(data);
    	}
        /*StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }*/
    	} 
 
 
 
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
 
 public static class WikiRecordReader extends RecordReader<LongWritable, Text>{
	    private final int NLINESTOPROCESS = 1;
	    private String tagStart = "<text xml:space=\"preserve\">";
	    private String tagEnd = "</text>";
	    private String pageStart = "<page>";
	    private String pageEnd = "<page>";
	    private boolean tagStarted = false;
	    private LineReader in;
	    private LongWritable key;
	    private Text value = new Text();
	    private long start =0;
	    private long end =0;
	    private long pos =0;
	    private int maxLineLength;
	    private String title = ":=unknown";
	 
	@Override
	    public void close() throws IOException {
	        if (in != null) {
	            in.close();
	        }
	    }
	 
	@Override
	    public LongWritable getCurrentKey() throws IOException,InterruptedException {
	        return key;
	    }
	 
	@Override
	    public Text getCurrentValue() throws IOException, InterruptedException {
	        return value;
	    }
	 
	@Override
	    public float getProgress() throws IOException, InterruptedException {
	        if (start == end) {
	            return 0.0f;
	        }
	        else {
	            return Math.min(1.0f, (pos - start) / (float)(end - start));
	        }
	    }
	 
	@Override
	    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
	        FileSplit split = (FileSplit) genericSplit;
	        final Path file = split.getPath();
	        Configuration conf = context.getConfiguration();
	        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
	        FileSystem fs = file.getFileSystem(conf);
	        start = split.getStart();
	        end= start + split.getLength();
	        boolean skipFirstLine = false;
	        FSDataInputStream filein = fs.open(split.getPath());
	 
	        if (start != 0){
	            skipFirstLine = true;
	            --start;
	            filein.seek(start);
	        }
	        in = new LineReader(filein,conf);
	        if(skipFirstLine){
	            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
	        }
	        this.pos = start;
	    }
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        
        final Text endline = new Text("\n");
        int newSize = 0;
        boolean cont = true;
        //reading one line
        while (cont){
        	//value.clear();
        	Text v = new Text();
            while (pos < end) {
                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                value.append(v.getBytes(),0, v.getLength());
                value.append(endline.getBytes(),0, endline.getLength());
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < maxLineLength) {
                    break;
                }
            }
            /*
            String l = value.toString();
            //continue in case it is title
            if (l.contains("<title>")){
            	int start = l.indexOf("<title>")+7;
	        	int end = l.indexOf("</title>");
	        	title = ":="+l.substring(start, end+1);
	        	cont = true;
	        	value.clear();
            }
            //if it contains tagStart
            else if (l.contains(tagStart)){
            	//the text tag has started
            	//value.append(title.getBytes(), 0, title.getBytes().length);
            	if (l.contains(tagEnd)){
            		tagStarted = false;
            		cont = false;
            		value.append(title.getBytes(), 0, title.getBytes().length);
            	}
            	else {
            		tagStarted = true;
            		cont = true;
            	}
            	//cont = false;
            }
            else if (tagStarted){
            	value.append(title.getBytes(), 0, title.getBytes().length);
            	if (l.contains(tagEnd)){
            		tagStarted = false;
            		cont = false;
            		value.append(title.getBytes(), 0, title.getBytes().length);
            	}
            	//cont = false;
            }*/
            cont = false;
        }
        
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
	}
	//@Override
	public boolean nextKeyValue3() throws IOException, InterruptedException {
		if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;
        boolean cont = true;
        //reading one line
        while (cont){
        	//value.clear();
        	Text v = new Text();
            while (pos < end) {
                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                value.append(v.getBytes(),0, v.getLength());
                value.append(endline.getBytes(),0, endline.getLength());
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < maxLineLength) {
                    break;
                }
            }
            String l = value.toString();
            //continue in case it is title
            if (l.contains("<title>")){
            	int start = l.indexOf("<title>")+7;
	        	int end = l.indexOf("</title>");
	        	title = ":="+l.substring(start, end+1);
	        	cont = true;
	        	value.clear();
            }
            //if it contains tagStart
            else if (l.contains(tagStart)){
            	//the text tag has started
            	//value.append(title.getBytes(), 0, title.getBytes().length);
            	if (l.contains(tagEnd)){
            		tagStarted = false;
            		cont = false;
            		value.append(title.getBytes(), 0, title.getBytes().length);
            	}
            	else {
            		tagStarted = true;
            		cont = true;
            	}
            	//cont = false;
            }
            else if (tagStarted){
            	value.append(title.getBytes(), 0, title.getBytes().length);
            	if (l.contains(tagEnd)){
            		tagStarted = false;
            		cont = false;
            		value.append(title.getBytes(), 0, title.getBytes().length);
            	}
            	//cont = false;
            }
        }
        
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
	}
	//@Override
	    public boolean nextKeyValue2() throws IOException, InterruptedException {
	        if (key == null) {
	            key = new LongWritable();
	        }
	        key.set(pos);
	        if (value == null) {
	            value = new Text();
	        }
	        value.clear();
	        final Text endline = new Text("\n");
	        int newSize = 0;
	        //boolean tagEnd = false;
	        
	        /*added by Anurag*/
	        /*
	        while (!tagEnd){
		        Text v = new Text();
	            while (pos < end) {
	                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
	                value.append(v.getBytes(),0, v.getLength());
	                value.append(endline.getBytes(),0, endline.getLength());
	                if (newSize == 0) {
	                	tagEnd = true;
	                    break;
	                }
	                pos += newSize;
	                //if (newSize < maxLineLength) {
	                	//tagEnd = true;
	                //    break;
	                //}
	                String curLine = v.toString();
		            if (curLine.indexOf("</page>")>=0) {
		            	tagEnd = true;
		            	break;
		            }
	            }
	            
	        }*/
	        /*Anurag code - end*/
	        
	        for(int i=0;i<NLINESTOPROCESS;i++){
	            Text v = new Text();
	            while (pos < end) {
	                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
	                value.append(v.getBytes(),0, v.getLength());
	                value.append(endline.getBytes(),0, endline.getLength());
	                if (newSize == 0) {
	                    break;
	                }
	                pos += newSize;
	                if (newSize < maxLineLength) {
	                    break;
	                }
	            }
	        }
	        String l = value.toString();
	        if (l.contains("<title>")){
	        	
	        	int start = l.indexOf("<title>")+7;
	        	int end = l.indexOf("</title>");
	        	title = ":="+l.substring(start, end+1);
	        }
	        value.append(title.getBytes(), 0, title.getBytes().length);;
	        //System.out.println("Input : "+value.toString());
	        if (newSize == 0) {
	            key = null;
	            value = null;
	            return false;
	        } else {
	            return true;
	        }
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
 }
        
}