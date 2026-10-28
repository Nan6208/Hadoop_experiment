
#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/pagerank-lab"

JAR="$HOME/pagerank-lab/pagerank-mr.jar"
INPUT="/pagerank/input/links.txt"

for i in $(seq 1 10); do
    OUTPUT="/pagerank/mr_iter_${i}"

    hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true

    echo "===== MapReduce 第 ${i} 轮 ====="

    hadoop jar "$JAR" PageRankMR "$INPUT" "$OUTPUT"

    INPUT="$OUTPUT/part-r-00000"
done

mkdir -p output

hdfs dfs -get -f "$INPUT" output/mr_internal.txt

awk -F '\t' '{
    printf "(%s,%.10f)\n", $1, $3
}' output/mr_internal.txt > output/mr_result.txt

echo "===== 最终结果 ====="
cat output/mr_result.txt
