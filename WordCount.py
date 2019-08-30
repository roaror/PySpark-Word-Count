from pyspark import SparkConf, SparkContext


if __name__ == "__main__":

	print("Reaching PySpark File")
	conf = SparkConf().setMaster("local").setAppName("My App")
	sc = SparkContext(conf = conf)
	sc.setLogLevel("Error");

	lines = sc.textFile("RealEstateTransactions.csv")

	words = lines.flatMap(lambda line: line.split(" "))

	wordCounts = words.countByValue()
	for word, count in wordCounts.items():
		print("{} : {}".format(word,count))

	sc.stop()