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
					//System.out.println(business_id.toString() + " : " + stars.toString());
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



	public static void main ( String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2){
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}


		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
			/*If exist delete the output path*/
			fs.delete(new Path(args[1]),true);
		}

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Top 10 Business Avg Ratings");

		job.setJarByClass(TopTenBusiness.class);



		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		//job.setNumReduceTasks(0);
		FileInputFormat.setMinInputSplitSize(job, 500000000);


		System.exit(job.waitForCompletion(true)? 1: 0);

	}
}