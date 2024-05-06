package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.common_pagerank.question.PageRankRunner;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer {
  private static final double D = 0.85;  // 阻尼系数
  @Override
  public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
      throws IOException, InterruptedException {
    // 页面总数
    int totalPage = context.getConfiguration().getInt(PageRankRunner.TOTAL_PAGE, 0);
    // 来自其他页面的总贡献值
    double total = 0;
    String[] pageInfo = null;

    for(ReducePageRankWritable value : values){
      String tag = value.getTag();
      // 贡献值求和
      if(tag.equals(ReducePageRankWritable.PR_L)){
        total += Double.parseDouble(value.getData());
      }
      // 页面信息存储
      else if(tag.equals(ReducePageRankWritable.PAGE_INFO)){
        pageInfo = value.getData().split(" ");
      }
    }

    double pageRank = (1 - D) / totalPage + D * total;  // 排名值
    // 判断是否收敛
    if(Math.abs(pageRank - Double.parseDouble(pageInfo[1])) <= PageRankRunner.DELTA){
      context.getCounter(PageRankRunner.GROUP_NAME, PageRankRunner.COUNTER_NAME).increment(1);
    }
    // 更新当前page数值
    pageInfo[1] = String.valueOf(pageRank);

    // 输出
    StringBuilder res = new StringBuilder();
    for(String data : pageInfo){
      res.append(data).append(" ");
    }
    context.write(new Text(res.toString()), NullWritable.get());
  }
}
