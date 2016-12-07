package io.ampool;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by adongre on 10/17/16.
 */
public class TagsInfo {
  private static final String USERID = "userId";
  private static final String MOVIEID = "movieId";
  private static final String TAG = "tag";
  private static final String TIMESTAMP = "timestamp";

  private int userId;
  private int movieId;
  private String tag;
  private String timestamp;

//  public static List<ReviewsInfo> constructReviewInfoObj(JSONArray jsonArray) {
//
//  }


  @Override
  public String toString() {
    return "TagsInfo{" +
            "userId=" + userId +
            ", movieId=" + movieId +
            ", tag='" + tag + '\'' +
            ", timestamp='" + timestamp + '\'' +
            '}';
  }

  public TagsInfo(int userId, int movieId, String tag, String timestamp) {
    this.userId = userId;
    this.movieId = movieId;
    this.tag = tag;
    this.timestamp = timestamp;
  }
}
