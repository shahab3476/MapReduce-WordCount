import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {


    public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

       String line  = lineText.toString();
       Text currentWord  = new Text();

       // split the line to words and return (word,1);
       
	   String words[] = line.split(" ");
	   for (int i = 0; i< words.length;i++){
		   if (words[i].isEmpty()) continue;
		   currentWord  = new Text(words[i]);
	       context.write(currentWord, new IntWritable(1));
	   }
    }
}    