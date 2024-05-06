package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureMapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class TransitiveClosureMapperImpl extends TransitiveClosureMapper {
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException{
    String[] tokens = value.toString().split("\\s");

    context.write(new Text(tokens[0]), new Text("p_" + tokens[1]));
    context.write(new Text(tokens[1]), new Text("c_" + tokens[0]));
  }
}
