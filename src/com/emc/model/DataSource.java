package com.emc.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";

    public static final String CONNECTION_STRING = "jdbc:sqlite:/Users/michael/Documents/Udemy/Java/Database-MusicDB/database/" +DB_NAME;

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

    private Connection conn;

    public boolean open(){
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            return true;
        } catch (SQLException e) {
            System.out.println("couldn't connect to database");
            e.printStackTrace();
        }
        return false;
    }

    public void close() {
        try {
            if(conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("couldn't close connections");
            e.printStackTrace();
        }
    }

    public List<Artist> queryArtist(OrderBy orderBy) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_ARTISTS);
        if(!orderBy.equals(OrderBy.NONE)){
            sb.append(" ORDER BY ").append(COLUMN_ARTISTS_NAME).append(" COLLATE NOCASE ");
            if(orderBy.equals(OrderBy.DESCENDING)){
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
        if(!orderBy.equals(OrderBy.NONE)){
            sb.append(" ORDER BY ").append(TABLE_ALBUMS).append(".").append(COLUMN_ALBUMS_NAME).append(" COLLATE NOCASE ");
            if(orderBy.equals(OrderBy.DESCENDING)){
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
        if(!orderBy.equals(OrderBy.NONE)){
            sb.append(" ORDER BY ").append(COLUMN_ARTIST_LIST_TITLE).append(" COLLATE NOCASE ");
            if(orderBy.equals(OrderBy.DESCENDING)){
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
                ArtistList artistList = new ArtistList(name,album,track,title);
                artistLists.add(artistList);
            }
            return artistLists;

        } catch (SQLException e) {
            System.out.println("query failed");
            e.printStackTrace();
            return null;
        }
    }
}
