package io.ampool;

import org.json.simple.JSONObject;

/**
 * Created by adongre on 10/17/16.
 */
public class RatingsInfo {

    private static final String USERID = "userId";
    private static final String MOVIEID = "movieId";
    private static final String RATING = "rating";

    private int userId;
    private int movieId;
    private double rating;


    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public double getRating() {
        return rating;
    }

    public RatingsInfo(int userId, int movieId, double rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
    }

  public static RatingsInfo constructRatingInfo(String line) {
    String [] lineParts = line.split(",");
    return new RatingsInfo(Integer.parseInt(lineParts[0]), Integer.parseInt(lineParts[1]),
            Double.parseDouble(lineParts[2]));
  }


    @Override
    public String toString() {
        return "RatingsInfo{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", rating=" + rating +
                '}';
    }
}
