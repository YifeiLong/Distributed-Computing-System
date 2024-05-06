package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class DifferenceMapperImpl extends DifferenceMapper {
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException{
    // 文件名
    String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    String[] lines = value.toString().split("\n");

    for(String line : lines){
      if(filename.contains("R")){
        context.write(new Text(line), new Text("R"));
      }
      else if(filename.contains("S")){
        context.write(new Text(line), new Text("S"));
      }
    }
  }
}
