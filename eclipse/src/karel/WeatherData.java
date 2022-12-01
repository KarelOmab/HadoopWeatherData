package karel;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class WeatherData {

	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		ArrayList<String> listData = new ArrayList<String>();
		String date = "2020-01-01";
		
		@Override
		public void map(LongWritable arg0, Text Value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
		    String type = conf.get("type");
			String line = Value.toString();
			
			if (!(line.length() == 0)) {
				
				String[] data = line.split(",");
				String datetime = data[0];
				String windSpeed = data[1];
				String windDir = data[2];
				String humidity = data[3];
				String temperature = data[4];
				String pressure = data[5];
				String uv = data[6];
				String new_date = datetime.split(" ")[0];
				
				if (type.equals("windSpeed")) {
					listData.add(windSpeed);
				} else if (type.equals("windDir")) {
					listData.add(windDir);
				} else if (type.equals("humidity")) {
					listData.add(humidity);
				} else if (type.equals("temperature")) {
					listData.add(temperature);
				} else if (type.equals("pressure")) {
					listData.add(pressure);
				} else if (type.equals("uv")) {
					listData.add(uv);
				}

				
				if (!date.equals(new_date)) {
					String listString = listData.stream().map(Object::toString)
	                        .collect(Collectors.joining(","));
					context.write(new Text(date), new Text(listString));
					
					listData.clear();
					date = new_date;
				}

			}
		}

	}

	public static class MaxTemperatureReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text Key, Iterable<Text> Values, Context context)
				throws IOException, InterruptedException {
			
			String listData = "";
			for (Text t : Values)
				listData += t.toString();

			String[] dataS = listData.split(",");
			Float sum = 0.0f;
			int k = dataS.length;
			
			for(String d : dataS) {
				try {
					sum += Float.parseFloat(d);
				} catch (Exception e) {
					k -= 1; //problem reading input..
				}
			}
				
			
			float avg = sum / k;
			String result = String.valueOf(avg);
			
			context.write(Key, new Text(result));
		}

	}

	public static void main(String[] args) throws Exception {

		
		if (args.length != 3) {
			
			System.out.println("Usage:hadoop jar <jar path> karel.WeatherData <data path> <(windSpeed, windDir, humidity, temperature, pressure, uv)> <hdfs output dir>");
			return;
		}
		
		ArrayList<String> supportedTypes = new ArrayList<String>(Arrays.asList("windSpeed", "windDir", "humidity", "temperature", "pressure", "uv"));
		String type = args[1];
		if (!supportedTypes.contains(type)) {
			System.out.println("Invalid weather-data type provided!");
			System.out.println("Supported types: " + "windSpeed, windDir, humidity, temperature, pressure, uv");
			return;
		}

		Configuration conf = new Configuration();
		conf.set("type", type);
		
		Job job = new Job(conf, "Karel's Weather Data Analysis");
		job.setJarByClass(WeatherData.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


		Path OutputPath = new Path(args[2]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, OutputPath);
		OutputPath.getFileSystem(conf).delete(OutputPath);

	
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
