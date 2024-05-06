package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.SortHelper;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortHelperImpl extends SortHelper {
  @Override
  public List<String> sortSeq(List<String> input){
    Collections.sort(input, new Comparator<String>() {
      @Override
      public int compare(String s1, String s2) {
        return s1.compareTo(s2);
      }
    });
    return input;
  }
}
