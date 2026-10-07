from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID, amountSpent)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine).reduceByKey(lambda x, y: x + y)

flipped = rdd.map(lambda x: (x[1], x[0]))  #flip the key and value so that we can sort by amount spent

results = flipped.sortByKey().collect()
for result in results:
    print(result)



# results = rdd.sortByKey().collect()  #add sortByKey() to sort the results by customerID if u like
# for result in results:
#     print(f"{result[0]}, {result[1]:.2f}")  #print the customerID and amount spent in a formatted way with 2 decimal places