package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinReducer;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PageRankJoinReducerImpl extends PageRankJoinReducer {
  @Override
  public void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context)
      throws IOException, InterruptedException {
    String page = "";
    String rank = "";
    for(ReduceJoinWritable value : values){
      // page
      if(value.getTag().equals("1")){
        page = value.getData();
      }
      // rank
      else if(value.getTag().equals("2")){
        rank = value.getData();
      }
    }
    context.write(new Text(key.toString() + " " + rank + " " + page), NullWritable.get());
  }
}
