from __future__ import print_function

import sys
from operator import add
from argparse import ArgumentParser

from pyspark.sql import SparkSession

def extract_author(line):
    fields = line.split('\t')
    return fields[6], fields[2]

def main(args):
    spark = SparkSession\
        .builder\
        .appName("TopAuthorPerYear")\
        .getOrCreate()

    # Read the dataset
    data = spark.read.text(args.input).rdd.map(lambda r: r[0])

    # Filter data to obtain those from Australia and USA
    data = data.filter(lambda line: "Australia" in line or "USA" in line)

    # Extract the year, author and content
    year_author = data.map(extract_author)\
                      .map(lambda x: ((x[0], x[1]), 1))\
                      .reduceByKey(lambda x, y: x + y)

    # Group data by year
    by_year = year_author.map(lambda x: (x[0][0], (x[0][1], x[1])))\
                         .groupByKey()

    # Fetch top 3 authors by publication count, for Australia and USA, for each year
    top_authors = by_year.mapValues(lambda x: sorted(x, key=lambda y: y[1], reverse=True))\
                         .mapValues(lambda x: [(y[0], y[1]) for y in x[:3]] + 
                                               [(y[0], y[1]) for y in x[3:] if y[1] == x[2][1]])

    # Write the output to a file
    top_authors.map(lambda x: str(x[0]) + '\t' + ';'.join([y[0] + ',' + str(y[1]) for y in x[1]]))\
               .saveAsTextFile(args.output)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Format: python SparkPubMed_3.py --input INPUT_PATH --output OUTPUT_PATH", file=sys.stderr)
        sys.exit(-1)
        
    parser = ArgumentParser(description='SparkPubMed Task 3')
    parser.add_argument('--input', "-i", type=str, help="Input path")
    parser.add_argument('--output', "-o", type=str, help="Output path")
    
    args = parser.parse_args()

    main(args)

    
    


