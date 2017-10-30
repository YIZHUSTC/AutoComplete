import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;     // args[2]
		@Override
		public void setup(Context context) {
			//how to get n-gram from command line?
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String sentence = value.toString();

			sentence = sentence.trim().toLowerCase();

			//how to remove useless elements?
			sentence = sentence.replaceAll("[^a-z]", " ");
			
			//how to separate word by space?
			String[] words = sentence.split("\\s+");
			
			//how to build n-gram based on array of words?
			if (words.length < 2) {
                Logger logger = LoggerFactory.getLogger(NGramLibraryBuilder.class);
                logger.info("Single word or non-word sentence.");
                return;
			}

			StringBuilder phrase;
            //  Build NGram at words[i, ... ,j] by iterating all possible phrases
			for (int i = 0; i < words.length -1; i++) {
				phrase = new StringBuilder();
				phrase.append(words[i]);
				for (int j = i + 1; j < words.length && j - i + 1 <= noGram; j++) {
				    phrase.append(" ");
				    phrase.append(words[j]);
				    String curr = phrase.toString().trim();
				    context.write(new Text(curr), new IntWritable(1));
                }
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//how to sum up the total count for each n-gram?
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}