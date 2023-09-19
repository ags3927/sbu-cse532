from __future__ import print_function

from operator import add

from pyspark.sql import SparkSession

def main():
    input = "gs://cse532-ags/mapaffil2018.tsv"
    output = "gs://cse532-ags/PubMedExtraOutput"

    spark = SparkSession\
        .builder\
        .appName("PubMedExtra")\
        .getOrCreate()

    sc = spark.sparkContext

    lines = spark.read.text(input).rdd.map(lambda r: r[0])

    # Map each line to a tuple of (year, 1)
    journals = lines.map(lambda line: line.split("\t")).map(lambda words: (words[7], 1))\
                        .reduceByKey(lambda x, y: x + y)

    sorted_journal=journals.sortBy(lambda x: x[1], ascending=False)

    top_5 = sorted_journal.take(5)

    df = spark.read.option("delimiter", "\t").csv(input, header=False)

    flv1 = df.filter((df._c7 == top_5[0][0]))
    flv2 = df.filter((df._c7 == top_5[1][0]))
    flv3 = df.filter((df._c7 == top_5[2][0]))
    flv4 = df.filter((df._c7 == top_5[3][0]))
    flv5 = df.filter((df._c7 == top_5[4][0]))

    r1 = flv1.groupBy("_c6","_c7").count().sort("count", ascending=True).take(1)
    r2 = flv2.groupBy("_c6","_c7").count().sort("count", ascending=True).take(1)
    r3= flv3.groupBy("_c6","_c7").count().sort("count", ascending=True).take(1)
    r4 = flv4.groupBy("_c6","_c7").count().sort("count", ascending=True).take(1)
    r5 = flv5.groupBy("_c6","_c7").count().sort("count", ascending=True).take(1)

    flist=[]
    flist.append(r1)
    flist.append(r2)
    flist.append(r3)
    flist.append(r4)
    flist.append(r5)
    
    finalrdd=sc.parallelize(flist,1) 
    finalrdd.map(lambda x: "{0}\t{1}".format(x[0][0], x[0][1])).saveAsTextFile(output)
    
    spark.stop()

if __name__ == "__main__":
    main()
