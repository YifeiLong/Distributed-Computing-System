package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FrequentItemAnalysisReducerImpl extends FrequentItemAnalysisReducer {
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException{
    // 获得总数和支持度
    Configuration conf = context.getConfiguration();
    int total = conf.getInt("count.of.transactions", 1);
    double support = conf.getDouble("support", 0.0);
    int cnt = 0;
    for(IntWritable value : values){
      cnt += value.get();
    }
    if(cnt >= (total * support)){
      context.write(new Text(key), NullWritable.get());
    }
  }
}
