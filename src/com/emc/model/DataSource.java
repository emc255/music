package com.emc.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";

    public static final String CONNECTION_STRING = "jdbc:sqlite:/Users/michael/Documents/Udemy/Java/Database-MusicDB/database/" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUMS_ID = "_id";
    public static final String COLUMN_ALBUMS_NAME = "name";
    public static final String COLUMN_ALBUMS_ARTIST = "artist";
    public static final int INDEX_ALBUMS_ID = 1;
    public static final int INDEX_ALBUMS_NAME = 2;
    public static final int INDEX_ALBUMS_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTISTS_ID = "_id";
    public static final String COLUMN_ARTISTS_NAME = "name";
    public static final int INDEX_ARTISTS_ID = 1;
    public static final int INDEX_ARTISTS_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONGS_ID = "id";
    public static final String COLUMN_SONGS_TRACK = "track";
    public static final String COLUMN_SONGS_TITLE = "title";
    public static final String COLUMN_SONGS_ALBUM = "album";
    public static final int INDEX_SONGS_ID = 1;
    public static final int INDEX_SONGS_TRACK = 2;
    public static final int INDEX_SONGS_TITLE = 3;
    public static final int INDEX_SONGS_ALBUM = 4;

    public static final String TABLE_ARTIST_LIST = "artist_list";
    public static final String COLUMN_ARTIST_LIST_ARTIST = "artist";
    public static final String COLUMN_ARTIST_LIST_ALBUM = "album";
    public static final String COLUMN_ARTIST_LIST_TRACK = "track";
    public static final String COLUMN_ARTIST_LIST_TITLE = "title";

    //right way to sql to avoid sql injection attack
    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT * FROM "
            + TABLE_ARTIST_LIST + " WHERE " + COLUMN_ARTIST_LIST_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS +
            "(" + COLUMN_ARTISTS_NAME + ")" +
            " VALUES (?)";

    public static final String INSERT_ALBUM = "INSERT INTO " + TABLE_ALBUMS +
            "(" + COLUMN_ALBUMS_NAME + ", " + COLUMN_ALBUMS_ARTIST + ")" +
            " VALUES (?,?)";

    public static final String INSERT_SONG = "INSERT INTO " + TABLE_SONGS +
            "(" + COLUMN_SONGS_TRACK + ", " + COLUMN_SONGS_TITLE + ", " + COLUMN_SONGS_ALBUM + ")" +
            " VALUES (?,?,?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTISTS_ID +
            " FROM " + TABLE_ARTISTS +
            " WHERE " + COLUMN_ARTISTS_NAME + " = ?";

    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUMS_ID +
            " FROM " + TABLE_ALBUMS +
            " WHERE " + COLUMN_ALBUMS_NAME + " = ?";

    public static final String QUERY_SONG = "SELECT *" +
            " FROM " + TABLE_ARTIST_LIST +
            " WHERE " + COLUMN_ARTIST_LIST_TITLE + " = ?" +
            " AND " + COLUMN_ARTIST_LIST_ARTIST + " = ?" +
            " AND " + COLUMN_ARTIST_LIST_ALBUM + " = ?" +
            " AND " + COLUMN_ARTIST_LIST_TRACK + " = ?";

    public static final String QUERY_SONG_TRACK_BY_ALBUM = "SELECT " + COLUMN_SONGS_TRACK +
            " FROM " + TABLE_SONGS +
            " JOIN " + TABLE_ALBUMS +
            " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUMS_ID + " = " + TABLE_SONGS + "." + COLUMN_SONGS_ALBUM +
            " WHERE " + TABLE_ALBUMS + "." + COLUMN_ALBUMS_NAME + " = ?" +
            " AND " + TABLE_SONGS + "." + COLUMN_SONGS_TRACK + " = ?";


    private Connection conn;
    private PreparedStatement querySongInfoView;

    private PreparedStatement insertIntoArtist;
    private PreparedStatement insertIntoAlbum;
    private PreparedStatement insertIntoSong;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement querySong;
    private PreparedStatement querySongTrackByAlbum;


    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            //initialize the prepared statement
            querySongInfoView = conn.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);

            insertIntoArtist = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbum = conn.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSong = conn.prepareStatement(INSERT_SONG);

            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);
            querySong = conn.prepareStatement(QUERY_SONG);
            querySongTrackByAlbum = conn.prepareStatement(QUERY_SONG_TRACK_BY_ALBUM);

            return true;
        } catch (SQLException e) {
            System.out.println("couldn't connect to database");
            e.printStackTrace();
        }
        return false;
    }

    public void close() {
        try {

            if (querySongInfoView != null) {
                querySongInfoView.close();
            }
            if (insertIntoArtist != null) {
                insertIntoArtist.close();
            }
            if (insertIntoAlbum != null) {
                insertIntoArtist.close();
            }
            if (insertIntoSong != null) {
                insertIntoSong.close();
            }
            if (queryArtist != null) {
                queryArtist.close();
            }
            if (queryAlbum != null) {
                queryAlbum.close();
            }
            if (querySong != null) {
                querySong.close();
            }
            if (querySongTrackByAlbum != null) {
                querySongTrackByAlbum.close();
            }

            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("couldn't close connections");
            e.printStackTrace();
        }
    }

    public List<Artist> queryArtist(OrderBy orderBy) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_ARTISTS);
        if (!orderBy.equals(OrderBy.NONE)) {
            sb.append(" ORDER BY ").append(COLUMN_ARTISTS_NAME).append(" COLLATE NOCASE ");
            if (orderBy.equals(OrderBy.DESCENDING)) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<Artist> artists = new ArrayList<>();
            while (results.next()) {
                Artist artist = new Artist(results.getInt(COLUMN_ARTISTS_ID), results.getString(COLUMN_ARTISTS_NAME));
                artists.add(artist);
            }
            return artists;

        } catch (SQLException e) {
            System.out.println("query failed");
            e.printStackTrace();
            return null;
        }
    }


    public List<String> queryAlbumsForArtist(String artistName, OrderBy orderBy) {
        StringBuilder sb = new StringBuilder("SELECT ").append(TABLE_ALBUMS).append(".").append(COLUMN_ARTISTS_NAME)
                .append(" FROM ").append(TABLE_ALBUMS)
                .append(" JOIN ").append(TABLE_ARTISTS)
                .append(" ON ").append(TABLE_ARTISTS).append(".").append(COLUMN_ARTISTS_ID).append(" = ").append(TABLE_ALBUMS).append(".").append(COLUMN_ALBUMS_ARTIST)
                .append(" WHERE ").append(TABLE_ARTISTS).append(".").append(COLUMN_ARTISTS_NAME).append(" = \"").append(artistName).append("\"");
        if (!orderBy.equals(OrderBy.NONE)) {
            sb.append(" ORDER BY ").append(TABLE_ALBUMS).append(".").append(COLUMN_ALBUMS_NAME).append(" COLLATE NOCASE ");
            if (orderBy.equals(OrderBy.DESCENDING)) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while (results.next()) {
                albums.add(results.getString(COLUMN_ALBUMS_NAME));
            }
            return albums;

        } catch (SQLException e) {
            System.out.println("query failed");
            e.printStackTrace();
            return null;
        }
    }

    public List<ArtistList> queryArtistsForSong(String songName, OrderBy orderBy) {
        StringBuilder sb = new StringBuilder("SELECT *")
                .append(" FROM ").append(TABLE_ARTIST_LIST)
                .append(" WHERE ").append(COLUMN_ARTIST_LIST_TITLE).append(" = \"").append(songName).append("\"");
        if (!orderBy.equals(OrderBy.NONE)) {
            sb.append(" ORDER BY ").append(COLUMN_ARTIST_LIST_TITLE).append(" COLLATE NOCASE ");
            if (orderBy.equals(OrderBy.DESCENDING)) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<ArtistList> artistLists = new ArrayList<>();
            while (results.next()) {
                String name = results.getString(COLUMN_ARTIST_LIST_ARTIST);
                String album = results.getString(COLUMN_ARTIST_LIST_ALBUM);
                int track = results.getInt(COLUMN_ARTIST_LIST_TRACK);
                String title = results.getString(COLUMN_ARTIST_LIST_TITLE);
                ArtistList artistList = new ArtistList(name, album, track, title);
                artistLists.add(artistList);
            }
            return artistLists;

        } catch (SQLException e) {
            System.out.println("query failed");
            e.printStackTrace();
            return null;
        }
    }

    //correct method to avoid sql injection attack
    public List<ArtistList> queryArtistsForSong(String songName) {
        try {
            querySongInfoView.setString(1, songName);
            ResultSet results = querySongInfoView.executeQuery();

            List<ArtistList> artistLists = new ArrayList<>();
            while (results.next()) {
                String name = results.getString(COLUMN_ARTIST_LIST_ARTIST);
                String album = results.getString(COLUMN_ARTIST_LIST_ALBUM);
                int track = results.getInt(COLUMN_ARTIST_LIST_TRACK);
                String title = results.getString(COLUMN_ARTIST_LIST_TITLE);
                ArtistList artistList = new ArtistList(name, album, track, title);
                artistLists.add(artistList);
            }
            return artistLists;

        } catch (SQLException e) {
            System.out.println("query failed");
            e.printStackTrace();
            return null;
        }
    }

    public void querySongsMetadata() {
        String sql = "SELECT * FROM " + TABLE_SONGS;

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sql)) {

            ResultSetMetaData meta = results.getMetaData();
            int numColumns = meta.getColumnCount();
            for (int i = 1; i <= numColumns; i++) {
                System.out.format("Column %d in the songs table is names %s\n",
                        i, meta.getColumnName(i));
            }
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
        }
    }

    public String getCount(String tableName) {
        String sql = "SELECT COUNT (*) FROM " + tableName;

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int count = resultSet.getInt(1);
            return count + " of " + tableName;
        } catch (SQLException e) {
            System.out.println("no " + tableName + " found");
            return "table name incorrect";
        }
    }

    private int insertArtist(String name) throws SQLException {
        queryArtist.setString(1, name);
        ResultSet result = queryArtist.executeQuery();

        if (result.next()) {
            return result.getInt(1);
        } else {
            insertIntoArtist.setString(1, name);
            int affectedRows = insertIntoArtist.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("couldn't insert artist");
            }

            ResultSet generatedKey = insertIntoArtist.getGeneratedKeys();
            if (generatedKey.next()) {
                return generatedKey.getInt(1);
            } else {
                throw new SQLException("couldn't get _id for an artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException {
        queryAlbum.setString(1, name);
        ResultSet result = queryAlbum.executeQuery();

        if (result.next()) {
            return result.getInt(1);
        } else {
            insertIntoAlbum.setString(1, name);
            insertIntoAlbum.setInt(2, artistId);
            int affectedRows = insertIntoAlbum.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("couldn't insert album");
            }

            ResultSet generatedKey = insertIntoAlbum.getGeneratedKeys();
            if (generatedKey.next()) {
                return generatedKey.getInt(1);
            } else {
                throw new SQLException("couldn't get _id for an album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {
        if(track <= 0) {
            System.out.println("invalid track number");
            return;
        }
        try {
            querySong.setString(1, title);
            querySong.setString(2, artist);
            querySong.setString(3, album);
            querySong.setInt(4, track);
            querySongTrackByAlbum.setString(1,album);
            querySongTrackByAlbum.setInt(2,track);
            ResultSet songResult = querySong.executeQuery();
            ResultSet songTrackResult = querySongTrackByAlbum.executeQuery();
            if(songResult.next()) {
                System.out.println(title+ " is already in " +album);
            } else if(songTrackResult.next()) {
                System.out.println("track number " +track+ " is already present in " +album+ " album");
            } else {
                try {
                    conn.setAutoCommit(false);
                    int artistId = insertArtist(artist);
                    int albumId = insertAlbum(album, artistId);

                    insertIntoSong.setInt(1, track);
                    insertIntoSong.setString(2, title);
                    insertIntoSong.setInt(3, albumId);

                    int affectedRows = insertIntoSong.executeUpdate();

                    if (affectedRows == 1) {
                        conn.commit();
                    } else {
                        throw new SQLException("song insert failed");
                    }

                } catch (Exception e) {
                    System.out.println("insert song exception");
                    e.printStackTrace();
                    try {
                        System.out.println("Performing Rollback");
                        conn.rollback();
                    } catch (SQLException e2) {
                        System.out.println("this is bad");
                    }
                } finally {
                    try {
                        System.out.println("Resetting default behavior");
                        conn.setAutoCommit(true);
                    } catch (SQLException e) {
                        System.out.println("could not reset auto commit");
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
