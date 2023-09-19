from __future__ import print_function

import sys
from operator import add
from argparse import ArgumentParser

from pyspark.sql import SparkSession

def main(args):
    spark = SparkSession\
        .builder\
        .appName("AnnualPublicationCount")\
        .getOrCreate()

    lines = spark.read.text(args.input).rdd.map(lambda r: r[0])

    # Read and map each line to a tuple of the form (YEAR, 1)
    years = lines.map(lambda line: line.split("\t")).map(lambda words: (words[6], 1))

    # Reduce by key to get the number of publications per year
    publication_count = years.reduceByKey(lambda x, y: x + y)

    publication_count.map(lambda x: "%s\t%s" % (x[0], x[1])).saveAsTextFile(args.output)   

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Format: python SparkPubMed_1.py --input INPUT_PATH --output OUTPUT_PATH", file=sys.stderr)
        sys.exit(-1)
        
    parser = ArgumentParser(description='SparkPubMed Task 1')
    parser.add_argument('--input', "-i", type=str, help="Input path")
    parser.add_argument('--output', "-o", type=str, help="Output path")
    
    args = parser.parse_args()

    main(args)

    