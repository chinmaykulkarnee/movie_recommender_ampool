package io.ampool;

/**
 * Created by adongre on 10/17/16.
 */
public class MoviesInfo {

  private int movieId;
  private String title;

  private final static String MOVIEID = "movieId";
  private final static String TITLE = "title";

  @Override
  public String toString() {
    return "MoviesInfo{" +
            "movieId=" + movieId +
            ", title='" + title + '\'' +
            '}';
  }

  public int getMovieId() {
    return movieId;
  }

  public String getTitle() {
    return title;
  }

  public MoviesInfo(int movieId, String title) {
    this.movieId = movieId;
    this.title = title;
  }

  public static MoviesInfo constructMoviesInfoObj(String line) {
    String [] lineParts = line.split(",");
    return new MoviesInfo(Integer.parseInt(lineParts[0]), lineParts[1]);
  }
}
