import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;		// args[3]

		@Override
		public void setup(Context context) {
			// how to get the threshold parameter from the configuration?
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				Logger logger = LoggerFactory.getLogger(LanguageModel.class);
				logger.info("Not a valid phrase.");
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				Logger logger = LoggerFactory.getLogger(LanguageModel.class);
				logger.info("Not a valid word with count.");
				return;
			}

			String phrase = wordsPlusCount[0].trim();
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threshold
			if(count < threshold) {
				return;
			}

			if (!phrase.contains(" ")) {
				Logger logger = LoggerFactory.getLogger(LanguageModel.class);
				logger.info("Single word phrase.");
				return;
			}
			
			//this is --> cool = 20
			String startingPhrase = phrase.substring(0, phrase.lastIndexOf(" ")).trim();
			String followingWord = phrase.substring(phrase.lastIndexOf(" ") + 1).trim();

			//what is the outputkey?
			//what is the outputvalue?
			
			//write key-value to reducer?
			if (startingPhrase != null && startingPhrase.length() > 0 &&
				followingWord != null && followingWord.length() > 0) {
				context.write(new Text(startingPhrase), new Text(followingWord + "=" + count));
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;		//  args[4]
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//  key : startingPhrase, values : list of followingWord=count
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			PriorityQueue<CountAndWord> priorityQueue = new PriorityQueue<CountAndWord> (n,
					new Comparator<CountAndWord>() {	//  MinHeap
						public int compare(CountAndWord c1, CountAndWord c2) {
							return c1.getCount() - c2.getCount();
						}
					}
			);
			for (Text value : values) {
				String currentFollowingWord = value.toString().trim();
				if (!currentFollowingWord.contains("=")) {
					Logger logger = LoggerFactory.getLogger(LanguageModel.class);
					logger.info("Not a valid followingWord=count.");
					continue;
				}

				String followingWord = currentFollowingWord.split("=")[0].trim();
				int count = Integer.parseInt(currentFollowingWord.trim().split("=")[1].trim());
				if (followingWord == null || followingWord.length() < 1) {
					Logger logger = LoggerFactory.getLogger(LanguageModel.class);
					logger.info("Not a valid followingWord.");
					continue;
				}

				//  for the same key (startingPhrase), store only topN frequent followingWord
				if (priorityQueue.isEmpty() || priorityQueue.size() < n) {
					CountAndWord countAndWord = new CountAndWord(count, followingWord);
					priorityQueue.offer(countAndWord);
				} else if (count > priorityQueue.peek().getCount()) {
					priorityQueue.poll();
					CountAndWord countAndWord = new CountAndWord(count, followingWord);
					priorityQueue.offer(countAndWord);
				}
			}

			while (!priorityQueue.isEmpty()) {
				int count = priorityQueue.peek().getCount();
				String word = priorityQueue.peek().getWord();
				context.write(new DBOutputWritable(key.toString(), word, count), NullWritable.get());
				priorityQueue.poll();
			}
		}
	}
}
