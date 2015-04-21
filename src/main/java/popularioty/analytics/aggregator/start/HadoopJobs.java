package popularioty.analytics.aggregator.start;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HadoopJobs {
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "runtime");
		// This decrease the number of times the ES and CB clients have to join the ES and CB clusters respectively
		//job.setNumReduceTasks(-1);
		job.setJarByClass(HadoopJobs.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		/*job.setMapOutputKeyClass(RuntimeKey.class);
		job.setMapOutputValueClass(RuntimeVote.class);
		
		
		job.setOutputKeyClass(RuntimeKey.class);
		job.setOutputValueClass(RuntimeEdge.class);

		job.setMapperClass(GenericEntityMapper.class);
		job.setReducerClass(GenericEntityReducer.class);*/
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}