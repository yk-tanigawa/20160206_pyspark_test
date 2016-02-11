from pyspark import SparkContext
import numpy as np

def mock_func(n):
    return np.array([n, n + 1], dtype = np.float_)

def main():
    sc = SparkContext("local[4]", "pyspark_test")

    print(np.array(sc.parallelize(range(1, 10)).map(mock_func).collect()))

if __name__ == "__main__":
    main()
