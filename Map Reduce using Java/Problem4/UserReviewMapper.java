import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("deprecation")
public class UserReviewMapper extends Mapper<LongWritable, Text, Text, Text>{

	private HashSet<String> business_id = new HashSet<>();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try{
			URI[] businessFiles = context.getCacheFiles();
			if(businessFiles != null && businessFiles.length > 0) {
				for(URI businessFile : businessFiles) {
					readFile(businessFile);
				}
			}
		} catch(IOException ex) {
			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
	}

	private void readFile(URI filePath) {
		try{
			String record = null;
			BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(filePath.getPath()).getName()));
			while((record = bufferedReader.readLine()) != null) {
				String[] fields = record.split("::");
				if(fields.length == 3){
					if(fields[1].contains("Stanford,")){
						business_id.add(fields[0].trim());
					}
					
				}

			}
		} catch(IOException ex) {
			System.err.println("Exception while reading stop words file: " + ex.getMessage());
		}
	}


	protected void map(LongWritable baseAddress, Text value, Context context) throws IOException, InterruptedException{
		Text user = new Text();
		Text rating = new Text();
		String line = value.toString();
		String[] fields = line.split("::");
		if(fields.length == 4){
			if(business_id.contains(fields[2].trim())){
				user.set(fields[1].trim());
				rating.set(fields[3].trim());
				context.write(user, rating);
			}
		}		
	}
}
