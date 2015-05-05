package popularioty.analytics.aggregator.start;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.elasticsearch.common.text.Text;

import popularioty.analytics.aggregator.mapreduce.AggregateMapper;
import popularioty.analytics.aggregator.mapreduce.AggregationReducer;
import popularioty.analytics.aggregator.writable.AggregationKey;
import popularioty.analytics.aggregator.writable.AggregationVote;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopJobs {
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "final-aggregation");
		// This decrease the number of times the ES and CB clients have to join the ES and CB clusters respectively
		//job.setNumReduceTasks(-1);
		job.setJarByClass(HadoopJobs.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		job.setMapOutputKeyClass(AggregationKey.class);
		job.setMapOutputValueClass(AggregationVote.class);
		
		
		job.setOutputKeyClass(AggregationKey.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(AggregateMapper.class);
		job.setReducerClass(AggregationReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}