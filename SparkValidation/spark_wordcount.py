from pyspark import SparkContext, SparkConf
import string
import glob

inputName = "input_wordcount/doc1.txt"
outputName = "output_wordcount/spark_output.txt"

def main():

	# Set up Spark
	conf = SparkConf().setAppName("Word Count")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
	# Read in File
	rawFile = sc.textFile(inputName)
	# Split the word
	words = rawFile.flatMap(lambda s: s.split(" "))

	# Count the frequency, 
	wordCount = words.map(lambda w: (w, 1)).reduceByKey(lambda c1, c2: c1+c2)
	wordCount.saveAsTextFile(outputName)

if __name__ == "__main__":
	main()