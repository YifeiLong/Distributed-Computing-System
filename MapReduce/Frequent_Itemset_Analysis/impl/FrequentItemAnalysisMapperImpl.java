package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FrequentItemAnalysisMapperImpl extends FrequentItemAnalysisMapper {
  @Override
  public void map(LongWritable key, Text value,Context context)
      throws IOException, InterruptedException{
    List<String> goods = Arrays.asList(value.toString().split(","));

    // 获得阶数
    Configuration conf = context.getConfiguration();
    int n = conf.getInt("number.of.pairs", 1);

    if(goods.size() < n){
      return;
    }

    // 获取n阶子集
    if(n == 1){
      for(String good : goods){
        context.write(new Text(good), new IntWritable(1));
      }
    }
    else{
      // 排序
      SortHelperImpl sortHelper = new SortHelperImpl();
      goods = sortHelper.sortSeq(goods);
      String[] arr = new String[goods.size()];
      goods.toArray(arr);
      List<String[]> subsets = freqSet(arr, n);
      for(String[] subset : subsets){
        String res = String.join(",", subset);
        context.write(new Text(res), new IntWritable(1));
      }
    }
  }

  public static List<String[]> freqSet(String[] arr, int k){
    List<String[]> res = new ArrayList<>();
    if(k > arr.length || arr.length == 0 || k <= 0){
      return res;
    }
    freqSetHelper(arr, 0, k, new String[k], res);
    return res;
  }

  private static void freqSetHelper(String[] arr, int index, int k, String[] current, List<String[]> res){
    if(k == 0){
      res.add(current.clone());
      return;
    }
    for(int i = index; i <= arr.length - k; i ++){
      current[current.length - k] = arr[i];
      freqSetHelper(arr, i + 1, k - 1, current, res);
    }
  }
}
