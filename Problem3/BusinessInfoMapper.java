import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class BusinessInfoMapper extends Mapper<LongWritable, Text, Text, Text>  {
	static String record = "";

	@Override
	protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException{

		Text business_id = new Text();
		Text value = new Text();

		record = record.concat(line.toString());
		String[] fields = record.split("::");
		if(fields.length == 3){
			business_id.set(fields[0].trim());
			value.set(fields[1] + "\t" + fields[2]);
			context.write(business_id, value);
		}
		record = "";
	}
}
