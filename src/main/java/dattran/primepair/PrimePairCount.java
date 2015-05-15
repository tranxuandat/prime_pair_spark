package dattran.primepair;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PrimePairCount {

	@SuppressWarnings("serial")
	private static final FlatMapFunction<String, MyPair> WORDS_EXTRACTOR = new FlatMapFunction<String, MyPair>() {
		@Override
		public Iterable<MyPair> call(String s) throws Exception {
			String[] strList = s.split(" ");
			ArrayList<MyPair> result = new ArrayList<MyPair>();
			for (int i = 0 ; i < strList.length-1; i++) {
				try{
					int left = Integer.parseInt(strList[i]);
					int right = Integer.parseInt(strList[i+1]);
					MyPair pair = new MyPair(left, right);
					if (pair.isPrimePair()) {
						result.add(pair);
					}
				} catch (Exception e) {
					
				}
			}
			return result;
		}
	};

	@SuppressWarnings("serial")
	private static final PairFunction<MyPair, MyPair, Integer> WORDS_MAPPER = new PairFunction<MyPair, MyPair, Integer>() {
		@Override
		public Tuple2<MyPair, Integer> call(MyPair s) throws Exception {
			return new Tuple2<MyPair, Integer>(s, 1);
		}
	};

	@SuppressWarnings("serial")
	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err
					.println("Please provide the input file full path as argument");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName(
				"dattran.primepair.PrimePairCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context.textFile(args[0]);
		JavaRDD<MyPair> words = file.flatMap(WORDS_EXTRACTOR);
		JavaPairRDD<MyPair, Integer> pairs = words.mapToPair(WORDS_MAPPER);
		JavaPairRDD<MyPair, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
		try {
		    File fileOutput = new File(args[1]);
		    FileUtils.deleteDirectory(fileOutput);
		} catch (Exception e) {
			
		}
		counter.saveAsTextFile(args[1]);
		context.close();
	}
}
