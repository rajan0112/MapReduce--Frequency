import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CovidTestsDriver {
        public static void main(String[] args) throws Exception {
                if (args.length != 2) {
                        System.err.println("Usage: CovidTests <input path> <output path>");
                        System.exit(-1);
                }
                Job job = Job.getInstance();
                job.setNumReduceTasks(1);               
                job.setJarByClass(CovidTestsDriver.class);
                job.setJobName("CovidTests");
		FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.setMapperClass(CovidTestsMapper.class);
                job.setReducerClass(CovidTestsReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
