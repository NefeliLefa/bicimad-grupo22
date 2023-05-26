from pyspark import SparkContext, SparkConf

from random import random
FILES = [
    "/public_data/bicimad/201704_movements.json",
    "/public_data/bicimad/201705_movements.json",
    "/public_data/bicimad/201706_movements.json",
    "/public_data/bicimad/201707_movements.json",
    "/public_data/bicimad/201708_movements.json",
    "/public_data/bicimad/201709_movements.json",
    "/public_data/bicimad/201710_movements.json",
    "/public_data/bicimad/201711_movements.json",
    "/public_data/bicimad/201712_movements.json",
    "/public_data/bicimad/201801_movements.json",
    "/public_data/bicimad/201802_movements.json",
    "/public_data/bicimad/201803_movements.json",
    "/public_data/bicimad/201804_movements.json",
    "/public_data/bicimad/201805_movements.json",
    "/public_data/bicimad/201806_movements.json"]


def init_sc(num):
    conf = SparkConf().setAppName(f'Bicimad Sample {num}')
    sc = SparkContext(conf=conf)
    return sc
    
def main(sc, num):
    rddlst = []
    for f in FILES:
        rddlst.append(sc.textFile(f'hdfs://{f}'))

    rdd = sc.union(rddlst)
    lst = rdd.takeSample(False, num)
    with open(f"sample_{num}.json", 'w') as fout:
        for data in lst:
            fout.write(f'{data}\n')

if __name__ == "__main__":
    import sys
    num = int(sys.argv[1])
    with init_sc(num,) as sc:
        main(sc, num)
