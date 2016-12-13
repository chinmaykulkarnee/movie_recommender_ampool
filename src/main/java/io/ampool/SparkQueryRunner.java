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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

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
//        ratingsInfoDF.show();

        DataFrame[] dataFrames = ratingsInfoDF.randomSplit(new double[]{6, 2, 2}, 0);
        JavaRDD<Rating> trainingRDD = convertDFtoRatingsRDD(dataFrames[0]);
        JavaRDD<Rating> validationRDD = convertDFtoRatingsRDD(dataFrames[1]);
        JavaRDD<Rating> testRDD = convertDFtoRatingsRDD(dataFrames[2]);

        MatrixFactorizationModel model = buildRecommendationModel(trainingRDD, validationRDD);

//        double testError = evaluateModel(testRDD, model);
//        System.out.println("The error for test data is " + testError);

        DataFrame moviesInfoDF = sqlContext.read().format("io.ampool").options(options).load(MOVIES_INFO_TABLE);
        JavaRDD<Tuple2<Integer, String>> moviesTitlesRDD = moviesInfoDF.javaRDD().map(
                (Function<Row, Tuple2<Integer, String>>) row -> new Tuple2<>(row.getInt(0), row.getString(1)));

//        getTopRatedMoviesForNewUser(jsc, ratingsInfoDF, moviesInfoDF, moviesTitlesRDD);

//        predictForAllExistingUsers(jsc, ratingsInfoDF, model, moviesInfoDF);

        JavaRDD<Integer> allUserIdsRDD = ratingsInfoDF.javaRDD().map(row -> row.getInt(0));
        JavaRDD<Integer> allMovieIdsRDD = moviesInfoDF.javaRDD().map(row -> row.getInt(0));
//        List<JavaRDD<Tuple2<Tuple2<Integer, Integer>, Double>>> predictions = allUserIdsRDD.top(3).stream().map(uid -> {
//            JavaRDD<Tuple2<Object, Object>> mapForPrediction = allMovieIdsRDD.map(mid -> new Tuple2<>(uid, mid));
//            return model.predict(JavaRDD.toRDD(mapForPrediction)).toJavaRDD()
//                    .map((Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>) r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()));
//        }).collect(Collectors.toList());
//
//        predictions.forEach(rdd -> System.out.println(rdd.take(1)));

//

    }

    private void predictForAllExistingUsers(JavaSparkContext jsc, DataFrame ratingsInfoDF, MatrixFactorizationModel model, DataFrame moviesInfoDF) {
        JavaPairRDD<Integer, ArrayList<Integer>> userIdsWithRatedMovieIds = ratingsInfoDF.javaRDD()
                .map(row -> new Tuple2<>(row.getInt(0), row.getInt(1)))
                .groupBy(tuple -> tuple._1)
                .mapValues(ls -> {
                    ArrayList<Integer> movieIds = new ArrayList<>();
                    ls.forEach(myTuple -> movieIds.add(myTuple._2));
                    return movieIds;
                });
        List<Row> rows = moviesInfoDF.collectAsList();
        JavaRDD<Tuple2<Integer, List<Row>>> userIdsWithUnratedMovieIds = userIdsWithRatedMovieIds.map(
                tuple -> new Tuple2<>(tuple._1, rows.stream().filter(
                        row -> !tuple._2.contains(row.getInt(0))).collect(Collectors.toList())));

        ArrayList<Integer> users = new ArrayList<>();
        users.add(536);
        users.add(356);

        userIdsWithUnratedMovieIds.filter(tuple -> users.contains(tuple._1)).foreach(tuple -> {
            int userId = tuple._1;
            ArrayList<Tuple2<Object, Object>> collect = tuple._2.stream()
                    .map(row -> new Tuple2<Object, Object>(userId, row.getInt(0)))
                    .collect(Collectors.toCollection(ArrayList::new));
            RDD<Tuple2<Object, Object>> tuple2RDD = JavaRDD.toRDD(jsc.parallelize(collect));
            RDD<Rating> userRecommendationsRDD = model.predict(tuple2RDD);
        });
    }

    private void getTopRatedMoviesForNewUser(JavaSparkContext jsc, DataFrame ratingsInfoDF, DataFrame moviesInfoDF, JavaRDD<Tuple2<Integer, String>> moviesTitlesRDD) {
        JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> moviesIdWithRatingsRDD = ratingsInfoDF.javaRDD()
                .map(row -> new Tuple2<>(row.getInt(1), row.getDouble(2)))
                .groupBy(tuple -> tuple._1);

        JavaRDD<Tuple2<Integer, Tuple2<Integer, Double>>> movieIdWithAvgRatingsRDD = moviesIdWithRatingsRDD
                .map(getMovieIdWithRatings());

        JavaRDD<Tuple2<Integer, Integer>> movieRatingCountsRDD = movieIdWithAvgRatingsRDD
                .map(tuple -> new Tuple2<>(tuple._1, tuple._2._1));

        int[][] newUserRatings = {
                {0, 260, 4}, // Star Wars (1977)
                {0, 1, 3}, // Toy Story (1995)
                {0, 16, 3}, // Casino (1995)
                {0, 25, 4}, // Leaving Las Vegas (1995)
                {0, 32, 4}, // Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
                {0, 335, 1}, // Flintstones, The (1994)
                {0, 379, 1}, // Timecop (1994)
                {0, 296, 3}, // Pulp Fiction (1994)
                {0, 858, 5}, // Godfather, The (1972)
                {0, 50, 4} // Usual Suspects, The (1995)
        };
        JavaRDD<Rating> newUserRatingsRDD = addNewUserRatings(jsc, newUserRatings);
        JavaRDD<Rating> allRatingsWithNewUserRatings = convertDFtoRatingsRDD(ratingsInfoDF).union(newUserRatingsRDD);

        MatrixFactorizationModel newModel = ALS.train(JavaRDD.toRDD(allRatingsWithNewUserRatings), 4, 10, 0.01);

        List newUserRatedMovieIds = getNewUserRatedMovieIds(newUserRatings);
        JavaRDD<Row> newUserUnratedMovies = moviesInfoDF.toJavaRDD()
                .filter(row -> !newUserRatedMovieIds.contains(row.getInt(0)));

        RDD<Rating> newUserRecommendationsRDD = newModel.predict(JavaRDD.toRDD(newUserUnratedMovies
                .map((Function<Row, Tuple2<Object, Object>>) row -> new Tuple2<>(0, row.getInt(0)))));

        JavaRDD<Tuple2<Integer, Double>> newUserRecommendationsRatingsRDD = newUserRecommendationsRDD.toJavaRDD()
                .map((Function<Rating, Tuple2<Integer, Double>>) r -> new Tuple2<>(r.product(), r.rating()));

        JavaPairRDD<Integer, Tuple2<Tuple2<Double, String>, Integer>> newUserRecommendationsRatingTitleAndCountRDD =
                JavaPairRDD.fromJavaRDD(newUserRecommendationsRatingsRDD)
                .join(JavaPairRDD.fromJavaRDD(moviesTitlesRDD))
                .join(JavaPairRDD.fromJavaRDD(movieRatingCountsRDD));

        JavaRDD<Tuple2<Double, String>> filteredMovieTitleAndRatingsRDD = newUserRecommendationsRatingTitleAndCountRDD.
                filter(tuple -> tuple._2._2 > 25).
                map(tuple -> new Tuple2<>(tuple._2._1._1, tuple._2._1._2));

        System.out.println(filteredMovieTitleAndRatingsRDD.
                sortBy(doubleStringTuple2 -> doubleStringTuple2._1, false, 1).take(5));
    }

    private List<Integer> getNewUserRatedMovieIds(int[][] newUserRatings) {
        return Arrays.asList(newUserRatings).parallelStream().map(arr -> arr[1]).collect(Collectors.toList());
    }

    private JavaRDD<Rating> addNewUserRatings(JavaSparkContext jsc, int[][] newUserRatings) {
        return jsc.parallelize(Arrays.asList(newUserRatings)).map(arr -> new Rating(arr[0], arr[1], arr[2]));
    }

    private Function<Tuple2<Integer, Iterable<Tuple2<Integer, Double>>>, Tuple2<Integer, Tuple2<Integer, Double>>> getMovieIdWithRatings() {
        return idRatingsTuple -> {
            int noOfRatings = 0;
            double sumOfRatings = 0, avgRating = 0;
            for (Tuple2<Integer, Double> tuple : idRatingsTuple._2) {
                sumOfRatings += tuple._2;
                noOfRatings++;
            }
            avgRating = sumOfRatings / noOfRatings;
            return new Tuple2<>(idRatingsTuple._1, new Tuple2<>(noOfRatings, avgRating));
        };
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
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = predictOnGivenRatingsRDD(ratingJavaRDD, model).values();

        return Math.sqrt(JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                (Function<Tuple2<Double, Double>, Object>) pair -> {
                    Double err = pair._1() - pair._2();
                    return err * err;
                }
        ).rdd()).mean());
    }

    private JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> predictOnGivenRatingsRDD(JavaRDD<Rating> ratingJavaRDD, MatrixFactorizationModel model) {
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
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> join = JavaPairRDD.fromJavaRDD(ratingJavaRDD.map(
                (Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>) r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())
        )).join(predictions);

        return join;
    }

    public static void main(String[] args) {
        new SparkQueryRunner().run();
    }
}
