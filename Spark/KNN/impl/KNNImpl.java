package DSPPCode.spark.knn.impl;

import DSPPCode.spark.knn.question.Data;
import DSPPCode.spark.knn.question.KNN;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KNNImpl extends KNN {

  public KNNImpl(int k) {
    super(k);
  }

  @Override
  public JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
    return queryData.cartesian(trainData);
  }

  @Override
  public JavaPairRDD<Integer, Tuple2<Integer, Double>> calculateDistance(JavaPairRDD<Data, Data> data) {
    // 当前点id，训练集点分类 & 对应距离
    return data.mapToPair(pair -> {
      Data queryData = pair._1;
      Data trainData = pair._2;
      double distance = 0.0;
      for(int i = 0; i < queryData.x.length; i ++){
        distance += Math.pow(queryData.x[i] - trainData.x[i], 2);
      }
      distance = Math.sqrt(distance);
      return new Tuple2<>(queryData.id, new Tuple2<>(trainData.y, distance));
    });
  }

  @Override
  public JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> data) {
    JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> combinedData = data.combineByKey(
        v -> {
          List<Tuple2<Integer, Double>> list = new ArrayList<>();
          list.add(v);
          return list;
        },
        (list, v) -> {
          list.add(v);
          if (list.size() > k) {
            list.sort(Comparator.comparing(Tuple2::_2));
            return new ArrayList<>(list.subList(0, k));
          } else {
            return list;
          }
        },
        (list1, list2) -> {
          list1.addAll(list2);
          list1.sort(Comparator.comparing(Tuple2::_2));
          if (list1.size() > k) {
            return new ArrayList<>(list1.subList(0, k));
          } else {
            return list1;
          }
        }
    );

    JavaPairRDD<Integer, Integer> finalResult = combinedData.mapToPair(pair -> {
      int queryId = pair._1;
      List<Tuple2<Integer, Double>> sortedDistance = pair._2;

      Map<Integer, Integer> votes = new HashMap<>();
      for (int i = 0; i < k && i < sortedDistance.size(); i++) {
        int label = sortedDistance.get(i)._1;
        votes.put(label, votes.getOrDefault(label, 0) + 1);
      }

      int maxVote = 0;
      int predictClass = -1;
      for (Map.Entry<Integer, Integer> entry : votes.entrySet()) {
        if (entry.getValue() > maxVote) {
          maxVote = entry.getValue();
          predictClass = entry.getKey();
        }
      }
      return new Tuple2<>(queryId, predictClass);
    });

    return finalResult;
  }
}
