package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class DifferenceReducerImpl extends DifferenceReducer {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int foundR = 0;
    int foundS = 0;

    for(Text value : values){
      if(value.toString().equals("R")){
        foundR += 1;
      }
      else if(value.toString().equals("S")){
        foundS += 1;
      }
    }
    if(foundR > foundS){
      context.write(key, NullWritable.get());
    }
  }
}
