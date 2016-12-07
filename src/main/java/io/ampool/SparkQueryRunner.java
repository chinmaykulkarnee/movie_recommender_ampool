package io.ampool;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by adongre on 10/17/16.
 */
public class SparkQueryRunner {
    private static String RATINGS_INFO_TABLE = "RatingsInfoTable";
    private static String MOVIES_INFO_TABLE = "MoviesInfoTable";

    private void run() {
        final String locatorHost = "localhost";
        final int locatorPort = 10334;

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkQueryRunner");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);


        Map<String, String> options = new HashMap<>(3);
        options.put("ampool.locator.host", locatorHost);
        options.put("ampool.locator.port", String.valueOf(locatorPort));

        DataFrame ratingsInfoDF = sqlContext.read().format("io.ampool").options(options).load(RATINGS_INFO_TABLE);
//        DataFrame moviesInfoDF = sqlContext.read().format("io.ampool").options(options).load(MOVIES_INFO_TABLE);

//        ratingsInfoDF.show();
//        moviesInfoDF.show();

        DataFrame[] dataFrames = ratingsInfoDF.randomSplit(new double[]{6, 2, 2}, 0);
        JavaRDD<Rating> trainingRDD = convertDFtoRatingsRDD(dataFrames[0]);
        JavaRDD<Rating> validationRDD = convertDFtoRatingsRDD(dataFrames[1]);
        JavaRDD<Rating> testRDD = convertDFtoRatingsRDD(dataFrames[2]);

        MatrixFactorizationModel model = buildRecommendationModel(trainingRDD, validationRDD);

        double testError = evaluateModel(testRDD, model);
        System.out.println("The error for test data is " + testError);

    }

    private MatrixFactorizationModel buildRecommendationModel(JavaRDD<Rating> trainingRDD, JavaRDD<Rating> validationRDD) {
        // Build the recommendation model using ALS
        int numIterations = 10;
        int [] ranks = new int [] {4, 8, 12};
        int bestRank = 0;
        double minError = Integer.MAX_VALUE;
        MatrixFactorizationModel model = null;

        for (int rank: ranks) {
            model = ALS.train(JavaRDD.toRDD(trainingRDD), rank, numIterations, 0.01);

            double error = evaluateModel(validationRDD, model);

            System.out.println("For rank " + rank + " the RMSE is " + error);
            if (error < minError) {
                minError = error;
                bestRank = rank;
            }
        }
        System.out.println("The best model was trained with rank " + bestRank + " & error is " + minError);
        return model;
    }

    private JavaRDD<Rating> convertDFtoRatingsRDD(DataFrame dataFrame) {
        return dataFrame.javaRDD().map(
                (Function<Row, Rating>) row -> new Rating(row.getInt(0), row.getInt(1), row.getDouble(2))
        );
    }

    private double evaluateModel(JavaRDD<Rating> ratingJavaRDD, MatrixFactorizationModel model) {
        JavaRDD<Tuple2<Object, Object>> validationForPredict = ratingJavaRDD.map(
                (Function<Rating, Tuple2<Object, Object>>) r -> new Tuple2<>(r.user(), r.product())
        );

        // Evaluate the model on validation data
        //(<userId,MovieId>, predictedRating)
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(validationForPredict)).toJavaRDD().map(
                        (Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>) r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())
                ));
        predictions.take(3);

        //(<userId,MovieId>, <actualRating,predictedRating>)
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(ratingJavaRDD.map(
                        (Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>) r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())
                )).join(predictions).values();

        return Math.sqrt(JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                (Function<Tuple2<Double, Double>, Object>) pair -> {
                    Double err = pair._1() - pair._2();
                    return err * err;
                }
        ).rdd()).mean());
    }

    public static void main(String[] args) {
        new SparkQueryRunner().run();
    }
}
