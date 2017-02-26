import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCategories {
	static HashSet<String> uniqueCategories = new HashSet<>();

	public static class UniqueCategoriesMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		static String record = "";


		@Override
		protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException {

			record = record.concat(line.toString());
			String[] fields = record.split("::");
			String categoryList = "";
			if(fields.length == 3){
				if(fields[1].contains("Palo Alto")){
					categoryList = fields[2];
					categoryList = categoryList.replace("List(", "").replace(")", "");
					String[] categoriesByRecord = categoryList.split(",");
					for(String s : categoriesByRecord){
						if(!uniqueCategories.contains(s.trim()) && !s.trim().equals("") ){
							uniqueCategories.add(s.trim());
							Text category = new Text();
							category.set(s.trim());
							NullWritable nullOb = NullWritable.get();
							context.write(category, nullOb);
						}

					}

				}
				record = "";
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
		Job job = new Job(conf, "Categories in Palo Alto");

		job.setJarByClass(UniqueCategories.class);



		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(UniqueCategoriesMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setMinInputSplitSize(job, 500000000);


		System.exit(job.waitForCompletion(true)? 1: 0);

	}
}