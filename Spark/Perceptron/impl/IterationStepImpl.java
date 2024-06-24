package DSPPCode.spark.perceptron.impl;

import DSPPCode.spark.perceptron.question.DataPoint;
import DSPPCode.spark.perceptron.question.IterationStep;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class IterationStepImpl extends IterationStep {

  @Override
  public Broadcast<double[]> createBroadcastVariable(JavaSparkContext sc, double[] localVariable) {
    return sc.broadcast(localVariable);
  }

  @Override
  public boolean termination(double[] old, double[] newWeightsAndBias) {
    double sum = 0;
    for (int i = 0; i < old.length; i++) {
      double diff = old[i] - newWeightsAndBias[i];
      sum += diff * diff;
    }
    return sum < THRESHOLD;
  }

  @Override
  public double[] runStep(JavaRDD<DataPoint> points, Broadcast<double[]> broadcastWeightsAndBias) {
    double[] weightsAndBias = broadcastWeightsAndBias.value();

    // Compute gradients
    JavaRDD<double[]> gradients = points
        .map(new ComputeGradient(weightsAndBias));

    // Sum gradients
    double[] totalGradient = gradients.reduce(new VectorSum());

    // Update weights and bias
    double[] newWeightsAndBias = new double[weightsAndBias.length];
    for (int i = 0; i < weightsAndBias.length; i++) {
      newWeightsAndBias[i] = weightsAndBias[i] + STEP * totalGradient[i];
    }

    return newWeightsAndBias;
  }

  public static class VectorSum extends IterationStep.VectorSum {
    @Override
    public double[] call(double[] a, double[] b) throws Exception {
      double[] result = new double[a.length];
      for (int i = 0; i < a.length; i++) {
        result[i] = a[i] + b[i];
      }
      return result;
    }
  }

  public static class ComputeGradient extends IterationStep.ComputeGradient {
    public ComputeGradient(double[] weightsAndBias) {
      super(weightsAndBias);
    }

    @Override
    public double[] call(DataPoint dataPoint) throws Exception {
      double[] gradient = new double[weightsAndBias.length];
      double y = dataPoint.y;
      double[] x = dataPoint.x;
      double wx = 0.0;
      for (int i = 0; i < x.length; i++) {
        wx += weightsAndBias[i] * x[i];
      }
      wx += weightsAndBias[weightsAndBias.length - 1];  // Bias term

      double prediction = wx >= 0 ? 1 : -1;

      if (y != prediction) {
        for (int i = 0; i < x.length; i++) {
          gradient[i] = y * x[i];
        }
        gradient[gradient.length - 1] = y;  // Gradient for bias
      }

      return gradient;
    }
  }
}
