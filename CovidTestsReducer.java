import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import javax.naming.Context;

public class CovidTestsReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long total_tests = 0;
     		 for (LongWritable value: values) {
			total_tests += value.get();
		}
		context.write(key, new LongWritable(total_tests));
	}
}
