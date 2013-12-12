package wiki;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class WikiRecordReader extends RecordReader<LongWritable, Text>{
    private final int NLINESTOPROCESS = 1;
    private String textStart = "<text xml:space=\"preserve\">";
    private String textEnd = "</text>";
    private boolean textStarted = false;
    private String pageStart = "<page>";
    private String pageEnd = "</page>";
    private String titleTagStart = "<title>";
    private String titleTagEnd = "</title>";
    private boolean pageStarted = false;
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
    pageStarted =false;
    //reading one line
    
    while (cont){
    	//value.clear();
    	Text v = new Text();
        while (pos < end) {
            newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }
        }
        String s = v.toString();
        if (s.contains(pageStart)){
        	cont = true;
        	pageStarted = true;
        }
        if (pageStarted){
        	if (s.contains(titleTagStart)){
        		//extract the title from the line and add it to "value"
        		String title ="title : "+ s.substring(s.indexOf("<title>")+7, s.indexOf("</")-1);
        		value.append(title.getBytes(), 0, title.length());
        		value.append(endline.getBytes(),0, endline.getLength());
        	}
            if (s.contains(textStart)){
            	textStarted = true;
            }
            if (textStarted){
        		//start logging in the data
        		value.append(v.getBytes(),0, v.getLength());
                value.append(endline.getBytes(),0, endline.getLength());
            }
            if (s.contains(textEnd)){
            	textStarted = false;
            }
        }
        
        if (s.contains(pageEnd)){
        	cont = false;
        	pageStarted = false;
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
}


