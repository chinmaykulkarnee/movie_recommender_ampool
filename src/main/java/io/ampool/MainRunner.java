package io.ampool;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.types.MBasicObjectType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MainRunner {

    private static String DATA_DIRECTORY = "/Users/chinmayk/Downloads/ml-latest-small/";

    private static String RATINGS_INFO_TABLE = "RatingsInfoTable";
    private static String MOVIES_INFO_TABLE = "MoviesInfoTable";

    private static int RATING_INFO_COUNT = 0;
    private static int MOVIES_INFO_COUNT = 0;

//    private static int BATCH_SIZE = 5000;

    private MClientCache clientCache = null;

    public static void main(String[] args) throws IOException, ParseException, java.text.ParseException, InterruptedException {
        MConfiguration mconf = MConfiguration.create();
        mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, "127.0.0.1");
        mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, 10334);
        //mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, "/tmp/MTableClient.log");

        new MainRunner().start(mconf);
    }

    private void ingestData() throws IOException, ParseException, java.text.ParseException {
        List<RatingsInfo> ratingsInfos = new ArrayList<>();
        List<MoviesInfo> moviesInfos = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(DATA_DIRECTORY + "ratings.csv")).parallel()) {
            ratingsInfos = stream
                    .map(RatingsInfo::constructRatingInfo)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (Stream<String> stream = Files.lines(Paths.get(DATA_DIRECTORY + "movies.csv")).parallel()) {
            moviesInfos = stream
                    .map(MoviesInfo::constructMoviesInfoObj)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        populateMTables(ratingsInfos, moviesInfos);

    }

    private void populateMTables(List<RatingsInfo> ratingsInfos, List<MoviesInfo> moviesInfos) throws java.text.ParseException {
        ratingsInfos.forEach(this::populateRatingsInfoTable);
        moviesInfos.forEach(this::populateMoviesInfoTable);
    }

    private void populateRatingsInfoTable(RatingsInfo ratingsInfo) {
        final MTable ratingsInfoTable = getRatingsInfoTable();

        MPut putRecord = new MPut(Bytes.toBytes(++RATING_INFO_COUNT));
        putRecord.addColumn("userId", ratingsInfo.getUserId());
        putRecord.addColumn("movieId", ratingsInfo.getMovieId());
        putRecord.addColumn("rating", ratingsInfo.getRating());

        ratingsInfoTable.put(putRecord);
    }

    private void populateMoviesInfoTable(MoviesInfo moviesInfo) {
        final MTable moviesInfoTable = getMoviesInfoTable();

        MPut putRecord = new MPut(Bytes.toBytes(++MOVIES_INFO_COUNT));
        putRecord.addColumn("movieId", moviesInfo.getMovieId());
        putRecord.addColumn("title", moviesInfo.getTitle());

        moviesInfoTable.put(putRecord);
    }

    private MTable getRatingsInfoTable() {
        MTable ratingsInfoTable = this.clientCache.getTable(RATINGS_INFO_TABLE);
        if (ratingsInfoTable == null) {
            MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
            tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
            tableDescriptor.addColumn("userId", MBasicObjectType.INT);
            tableDescriptor.addColumn("movieId", MBasicObjectType.INT);
            tableDescriptor.addColumn("rating", MBasicObjectType.DOUBLE);

            ratingsInfoTable = this.clientCache.getAdmin().createTable(RATINGS_INFO_TABLE, tableDescriptor);
        }
        return ratingsInfoTable;
    }

    private MTable getMoviesInfoTable() {
        MTable moviesInfoTable = this.clientCache.getTable(MOVIES_INFO_TABLE);
        if (moviesInfoTable == null) {
            MTableDescriptor tableDescriptor = new MTableDescriptor(MTableType.UNORDERED);
            tableDescriptor.enableDiskPersistence(MDiskWritePolicy.ASYNCHRONOUS);
            tableDescriptor.addColumn("movieId", MBasicObjectType.INT);
            tableDescriptor.addColumn("title", MBasicObjectType.STRING);

            moviesInfoTable = this.clientCache.getAdmin().createTable(MOVIES_INFO_TABLE, tableDescriptor);
        }
        return moviesInfoTable;
    }


    private void start(MConfiguration mconf) throws IOException, ParseException, java.text.ParseException, InterruptedException {
        this.clientCache = new MClientCacheFactory().create(mconf);
        ingestData();
        this.clientCache.close();
    }
}
