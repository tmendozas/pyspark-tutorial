from pyspark import SparkContext, SparkConf

logFile = "/usr/share/dict/words"

#Create spark context
sc = SparkContext("local", "Simple App")

#Read File
logData = sc.textFile(logFile)

#File sample
print "Total Records: %i" % logData.count()
print "Record Sample: %s" % logData.take(10)


#File operations
containsRai = logData.filter(lambda s: 'rai' in s)

print "Lines with rai: %i" % containsRai.count()

print "Record with rai Sample: %s" % containsRai.take(10)


# read File
textFile = sc.textFile("README.md") 

# Reduce operation (action)
longest = textFile.map(
		lambda line: len(line.split())
	).reduce(
		lambda a, b: a if (a > b) else b
	)

print longest

wordCounts = textFile.flatMap(
		lambda line: line.split()
	).map(
		lambda word: (word, 1)
	).reduceByKey(
		lambda a, b: a+b
	)