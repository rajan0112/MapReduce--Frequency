

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import javax.naming.Context;

public class CovidTestsMapper extends
    Mapper<LongWritable, Text, Text, LongWritable> {
 	@Override
	public void map(LongWritable key, Text value, Context context)
        	throws IOException, InterruptedException {
    	String line = value.toString();
    	String[] row = line.split(",");
    	String s = row[1];
    	String v = row[7];
	context.write(new Text(s), new LongWritable(Long.parseLong(v.trim())));
	}
    }
}

