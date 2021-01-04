from pyspark import SparkContext, SparkConf
import string
import glob

inputName = "input_grep/doc1.txt"
pattern = "of"
outputName = "output_grep/spark_output.txt"

def main():

	# Set up Spark
	conf = SparkConf().setAppName("Grep")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
	# Read in File
	rawFile = sc.textFile(inputName)

	# Count the frequency, 
	wordGrep = rawFile.filter(lambda s: pattern in s).map(lambda w: (inputName, w)).reduceByKey(lambda c1, c2: c1 + " " +c2)
	wordGrep.saveAsTextFile(outputName)

if __name__ == "__main__":
	main()