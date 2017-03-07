import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UserReview {

	public static void main ( String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3){
			System.err.println("Incompatible Number Of Arguments");
			System.exit(3);
		}
		
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[2]))){
		   /*If exist delete the output path*/
		   fs.delete(new Path(args[2]),true);
		}
		
		Path businessInputFile = new Path(otherArgs[0]);
		Path reviewInputFile = new Path(otherArgs[1]);
		Path outputFile = new Path(otherArgs[2]);
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "User Reviews for Business in Stanford");

		job.setJarByClass(UserReview.class);

		FileInputFormat.addInputPath(job, reviewInputFile);
		FileOutputFormat.setOutputPath(job, outputFile);
		job.addCacheFile(businessInputFile.toUri());
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(UserReviewMapper.class);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);

	}
}