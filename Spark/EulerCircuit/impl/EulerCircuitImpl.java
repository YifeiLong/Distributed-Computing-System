package DSPPCode.spark.eulercircuit.impl;

import DSPPCode.spark.eulercircuit.question.EulerCircuit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class EulerCircuitImpl extends EulerCircuit {
  @Override
  public boolean isEulerCircuit(JavaRDD<String> lines, JavaSparkContext jsc) {
    // 提取每行输入的起点、终点
    JavaPairRDD<Integer, Integer> edges = lines.mapToPair(line -> {
      String[] parts = line.split(" ");
      int from = Integer.parseInt(parts[0]);
      int to = Integer.parseInt(parts[1]);
      return new Tuple2<>(from, to);
    });

    // 顶点出度
    JavaPairRDD<Integer, Integer> outDegree = edges.mapToPair(
        edge -> new Tuple2<>(edge._1, 1)).reduceByKey(Integer::sum);
    // 顶点入度
    JavaPairRDD<Integer, Integer> inDegree = edges.mapToPair(
        edge -> new Tuple2<>(edge._2, 1)).reduceByKey(Integer::sum);
    // 合并出度和入度
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> degrees = outDegree.fullOuterJoin(inDegree)
        .mapValues(degree -> new Tuple2<>(degree._1.orElse(0), degree._2.orElse(0)));

    // 是否出度+入度为偶数
    boolean eulerCircuit = degrees.mapValues(degree -> degree._1 + degree._2).filter(
        degree -> degree._2 % 2 != 0).isEmpty();
    return eulerCircuit;
  }
}
