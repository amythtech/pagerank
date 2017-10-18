package com.project.pagerank;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

class Sum implements Function2<Double, Double, Double> {

	public Double call(Double a, Double b) {
		return a + b;
	}
}

public class PageRank {
	protected final static String PATTERN_STR1 = "(?<=<target>)(.?)(?=</target>)";
	protected final static String GETLINK = "get_links";
	protected final static String SPLIT = "\\t";
	protected final static String TEN = "10";

	public static void main(String[] args) {
		String inputFile = args[0];
		final Pattern pattern = Pattern.compile(PATTERN_STR1);
		SparkConf conf = new SparkConf().setAppName(GETLINK);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile(inputFile);

		JavaPairRDD<String, List<String>> makeLinks = input.mapToPair(new PairFunction<String, String, List<String>>() {

			public Tuple2<String, List<String>> call(String s) throws Exception {
				String[] components = s.split(SPLIT);
				Matcher matcher = pattern.matcher(s);
				ArrayList<String> link = new ArrayList<String>();
				while (matcher.find()) {
					link.add(matcher.group());
				}
				if (link.size() != 0)
					return new Tuple2<String, List<String>>(components[1], link);
				else
					return null;
			}

		}).cache();
		//refernece from spark example
		JavaPairRDD<String, List<String>> links = makeLinks
				.filter(new Function<Tuple2<String, List<String>>, Boolean>() {
					public Boolean call(Tuple2<String, List<String>> t) throws Exception {
						return t != null;
					}

				}).cache();
		JavaPairRDD<String, Double> ranks = links.mapValues(new Function<List<String>, Double>() {

			public Double call(List<String> rs) {
				return 1.0;
			}
		});

		for (int current = 0; current < Integer.parseInt(TEN); current++) {
			JavaPairRDD<String, Double> contribs = links.join(ranks).values()
					.flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>, Double>, String, Double>() {
						public List<Tuple2<String, Double>> call(Tuple2<List<String>, Double> s) {
							int urlCount = s._1.size();
							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
							for (String n : s._1) {
								results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
							}
							return results;
						}
					});
			ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
				public Double call(Double sum) {
					return 0.15 + sum * 0.85;
				}
			});
		}

		/*JavaPairRDD<Double, String> swappedPair = ranks
				.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
					public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
						return item.swap();
					}
				}).sortByKey(false);*/
	
		
		JavaPairRDD<Tuple2<Double, String>, Integer> countInKey = ranks.mapToPair(a -> new Tuple2(new Tuple2<Double, String>(a._2, a._1), null)); // setting value to null, since it won't be used anymore

	    List<Tuple2<Tuple2<Double, String>, Integer>> output = countInKey.sortByKey(false).collect();

		//List<Tuple2<Double, String>> output = swappedPair.collect();
		int k = 0;
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._2() + "  ::  " + tuple._1());
			k++;
			if (k > 100)
				break;
		}

		sc.stop();

	}
	
	

}