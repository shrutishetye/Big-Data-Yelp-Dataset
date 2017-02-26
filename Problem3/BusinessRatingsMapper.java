import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class BusinessRatingsMapper extends Mapper<LongWritable, Text, Text, Text>  {
	static String record = "";

	@Override
	protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException{

		Text business_id = new Text();
		Text value = new Text();

		record = record.concat(line.toString());
		String[] fields = record.split("\t");
		if(fields.length == 2){
			business_id.set(fields[0].trim());
			value.set("Average Ratings: " + fields[1].trim());
			context.write(business_id, value);
		}
		record = "";
	}
}
