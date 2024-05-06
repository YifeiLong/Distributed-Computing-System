package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisRunner;
import DSPPCode.mapreduce.frequent_item_analysis.impl.FrequentItemAnalysisCombinerImpl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.net.URISyntaxException;

public class FrequentItemAnalysisRunnerImpl extends FrequentItemAnalysisRunner {
  @Override
  public void configureMapReduceTask(Job job) throws IOException, URISyntaxException{
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setCombinerClass(FrequentItemAnalysisCombinerImpl.class);
  }
}
