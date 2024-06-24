package DSPPCode.spark.connected_components.impl;

import DSPPCode.spark.connected_components.question.ConnectedComponents;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectedComponentsImpl extends ConnectedComponents {
  @Override
  public JavaPairRDD<String, Integer> getcc(JavaRDD<String> text) {
    // 转化为顶点+相邻节点
    JavaPairRDD<String, List<String>> edges = text.mapToPair(line -> {
      String[] parts = line.split("\t");
      String vertex = parts[0];  // 当前顶点
      List<String> neighbor = new ArrayList<>(Arrays.asList(parts).subList(1, parts.length));  // 邻居节点
      return new Tuple2<>(vertex, neighbor);
    });

    // 初始化每个顶点最小连通分量id为自己
    JavaPairRDD<String, Integer> minVertex = edges.keys().mapToPair(
        vertex -> new Tuple2<>(vertex, Integer.parseInt(vertex)));

    boolean change = true;  // 是否需要继续迭代
    while(change){
      JavaPairRDD<String, Integer> minVertexNow = minVertex.join(edges).flatMapToPair(tuple -> {
        String vertex = tuple._1;  // 当前顶点
        int vertexNow = tuple._2._1;  // 当前对应最小连通分量顶点id
        List<String> neighbors = tuple._2._2;  // 当前相邻节点id
        // 将当前这里涉及到的所有节点对应的最小连通分量节点id均设置为当前默认的最小值
        List<Tuple2<String, Integer>> update = neighbors.stream()
            .map(neighbor -> new Tuple2<>(neighbor, vertexNow))
            .collect(Collectors.toList());
        update.add(new Tuple2<>(vertex, vertexNow));
        return update.iterator();
      }).reduceByKey(Math::min);  // 更新后，同一个顶点中取最小的

      change = isChange(minVertex, minVertexNow);
      minVertex = minVertexNow;
    }
    return minVertex;
  }
}
