from __future__ import print_function

import sys
from operator import add
from argparse import ArgumentParser

from pyspark.sql import SparkSession

def main(args):
    spark = SparkSession\
        .builder\
        .appName("PublicationCountSpecificYear")\
        .getOrCreate()

    df = spark.read.option("delimiter", "\t").csv(args.input, header=False)
    filtered_df = df.filter(df._c6 == args.year)
    result_df = filtered_df.groupBy("_c7").count().sort("count", ascending=False)
    result_df.rdd.map(lambda x: "{0}\t{1}".format(x[0], x[1])).saveAsTextFile(args.output)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Format: python SparkPubMed_2.py --input INPUT_PATH --year TARGET_YEAR --output OUTPUT_PATH", file=sys.stderr)
        sys.exit(-1)
        
    parser = ArgumentParser(description='SparkPubMed Task 2')
    parser.add_argument('--input', "-i", type=str, help="Input path")
    parser.add_argument("--year", "-y", type=str, help="Target year to count publications for")
    parser.add_argument('--output', "-o", type=str, help="Output path")
    
    args = parser.parse_args()

    main(args)

    
