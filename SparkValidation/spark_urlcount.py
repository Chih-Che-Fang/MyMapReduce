from pyspark import SparkContext, SparkConf
import string
import glob

inputName = "input_urlcount/page1.txt"
outputName = "output_urlcount/spark_output.txt"

def main():

	# Set up Spark
	conf = SparkConf().setAppName("URL Count")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
	# Read in File
	rawFile = sc.textFile(inputName)

	# Count the frequency, 
	urlCount = rawFile.map(lambda w: (w, 1)).reduceByKey(lambda c1, c2: c1+c2)
	urlCount.saveAsTextFile(outputName)

if __name__ == "__main__":
	main()