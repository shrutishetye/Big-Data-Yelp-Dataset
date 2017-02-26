import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends
Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text business_id, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		boolean isTopRated = false;
		String rating = "";
		String info = "";
		Text details = new Text();
		
		for( Text value : values){
			if(value.toString().contains("Average Ratings: ")){
				//rating = value.toString().split("\t")[1];
				rating = value.toString();
				isTopRated = true;
			} else{
				info = value.toString();
			}
		}
		
		if(!isTopRated){
			return;
		} else {
			details.set(info + "\t" + rating.replace("Average Ratings: ", ""));
			context.write(business_id, details);
		}
		
		
	}

}