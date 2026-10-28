
import sys
from pyspark.sql import SparkSession

DAMPING = 0.85
BASE = 0.15
ITERATIONS = 10


def parse_line(line):
    fields = line.rstrip("\n").split("\t")

    if len(fields) != 2:
        raise ValueError(f"输入格式错误：{line!r}")

    page = fields[0].strip()
    links = [
        x.strip()
        for x in fields[1].split(",")
        if x.strip()
    ]

    return page, links


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: spark-submit pagerank_spark.py "
            "<input> <output>",
            file=sys.stderr
        )
        sys.exit(2)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("PageRankSpark")
        .master("local[*]")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    try:
        # 读取网页及其出链。
        graph = (
            sc.textFile(input_path)
            .map(parse_line)
            .collectAsMap()
        )

        if not graph:
            raise ValueError("输入数据为空")

        # 检查所有被引用的网页都已在图中定义。
        pages = set(graph.keys())
        for page, links in graph.items():
            missing = set(links) - pages
            if missing:
                raise ValueError(
                    f"网页 {page} 指向未定义网页：{sorted(missing)}"
                )

        # 实验样例中每个网页都必须至少有一条出链。
        dangling = [
            page for page, links in graph.items()
            if not links
        ]
        if dangling:
            raise ValueError(
                f"发现没有出链的网页：{dangling}；"
                "本实现要求每个网页至少有一条出链"
            )

        graph_rdd = sc.parallelize(list(graph.items())).cache()
        ranks = sc.parallelize(
            [(page, 1.0) for page in graph.keys()]
        ).cache()

        # 连续执行 10 轮 PageRank 迭代。
        for iteration in range(1, ITERATIONS + 1):
            contributions = graph_rdd.join(ranks).flatMap(
                lambda item: [
                    (dest, item[1][1] / len(item[1][0]))
                    for dest in item[1][0]
                ]
            )

            new_ranks = (
                contributions
                .reduceByKey(lambda a, b: a + b)
                .mapValues(lambda total: BASE + DAMPING * total)
                .cache()
            )

            ranks.unpersist()
            ranks = new_ranks

            print(f"========== Spark 第 {iteration} 轮 ==========")
            print(
                "网页数量:",
                ranks.count()
            )

        # 保存为两列格式：网页、PR值。
        output = (
            ranks
            .sortByKey()
            .map(
                lambda item: f"({item[0]},{item[1]:.10f})"
            )
        )

        output.saveAsTextFile(output_path)

        print("Spark PageRank 完成。")
        print("输出目录:", output_path)

        graph_rdd.unpersist()
        ranks.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
