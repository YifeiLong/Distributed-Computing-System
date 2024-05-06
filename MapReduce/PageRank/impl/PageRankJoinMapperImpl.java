package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinMapper;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class PageRankJoinMapperImpl extends PageRankJoinMapper {

  /**
   * 输出key为网页，value为tag + 对应字符串
   * 要区分文件，标记tag
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 获取键值对所属文件路径
    FileSplit split = (FileSplit) context.getInputSplit();
    String path = split.getPath().toString();

    // 只在第一个空格处分割，分割为两部分
    String[] data = value.toString().split(" ", 2);

    // 输出类
    ReduceJoinWritable writable = new ReduceJoinWritable();
    if(path.contains("pages")){
      writable.setTag(ReduceJoinWritable.PAGEINFO);
      writable.setData(data[1]);
    }
    else if(path.contains("ranks")){
      writable.setTag(ReduceJoinWritable.PAGERNAK);
      writable.setData(data[1]);
    }
    context.write(new Text(data[0]), writable);
  }
}
