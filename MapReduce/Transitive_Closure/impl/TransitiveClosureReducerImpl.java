package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureReducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransitiveClosureReducerImpl extends TransitiveClosureReducer {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException{
    List<String> child = new ArrayList<>();
    List<String> parent = new ArrayList<>();

    for(Text value : values){
      String str = value.toString();
      String str1 = str.substring(0, 1);  // 关系
      String str2 = str.substring(2);  // 名称
      if(str1.equals("p")){
        parent.add(str2);
      }
      else{
        child.add(str2);
      }
    }

    if(child.size() != 0 && parent.size() != 0){
      for(String c : child){
        for(String p : parent){
          context.write(new Text(c), new Text(p));
        }
      }
    }
  }
}
