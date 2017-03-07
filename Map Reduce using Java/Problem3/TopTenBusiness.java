import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenBusiness {

	public static class TopTenMapper extends
	Mapper<LongWritable, Text, Text, FloatWritable> {
		static String record = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text business_id = new Text();
			FloatWritable stars = new FloatWritable(1);

			record = record.concat(line.toString());
			String[] fields = record.split("::");
			if (fields.length == 4) {
				if (true) {
					business_id.set(fields[2].trim());
					stars.set(Float.parseFloat(fields[3].trim()));
					context.write(business_id, stars);
				}
				record = "";
			}
		}
	}

	public static class TopTenReducer extends
	Reducer<Text, FloatWritable, Text, FloatWritable> {

		HashMap<String, Float> map = new HashMap<String, Float>();

		@Override
		protected void reduce(Text business_id, Iterable<FloatWritable> stars,
				Context context) throws IOException, InterruptedException {

			FloatWritable average = new FloatWritable(0);
			float total = 0;
			int count = 0;
			for (FloatWritable star : stars) {
				total += star.get();
				count++;
			}

			float avg = total / count;
			average.set(avg);
			map.put(business_id.toString(), avg);
		}

		@Override
		protected void cleanup(
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
						throws IOException, InterruptedException {

			Map<String, Float> sortedMap = new TreeMap<String, Float>(
					new ValueComparator(map));
			sortedMap.putAll(map);
			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),
						new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}
	}



	public static void main ( String args[]) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4){
			System.err.println("Incompatible Number Of Arguments");
			System.exit(4);
		}


		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[2]))){
			/*If exist delete the output path*/
			fs.delete(new Path(args[2]),true);
		}
		
		if(fs.exists(new Path(args[3]))){
			/*If exist delete the output path*/
			fs.delete(new Path(args[3]),true);
		}

		Path inputFileForBusiness = new Path(otherArgs[0]);
		Path inputFileForReview = new Path(otherArgs[1]);
		Path finalOutputFile = new Path(otherArgs[2]);
		Path intermediateOutputFile = new Path(otherArgs[3]);
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "Top 10 Business Avg Ratings");

		job1.setJarByClass(TopTenBusiness.class);

		FileInputFormat.addInputPath(job1, inputFileForReview);
		FileOutputFormat.setOutputPath(job1, intermediateOutputFile);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FloatWritable.class);

		job1.setMapperClass(TopTenMapper.class);
		job1.setReducerClass(TopTenReducer.class);

		job1.waitForCompletion(true);

		//---------------------------------------------------
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "Joiner");
		
		job2.setJarByClass(TopTenBusiness.class);
		
		MultipleInputs.addInputPath(job2, intermediateOutputFile,
				TextInputFormat.class, BusinessRatingsMapper.class);
		
		MultipleInputs.addInputPath(job2, inputFileForBusiness, TextInputFormat.class,
				BusinessInfoMapper.class);
		FileOutputFormat.setOutputPath(job2, finalOutputFile);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setReducerClass(JoinReducer.class);
		
		job2.waitForCompletion(true);

		

	}
}