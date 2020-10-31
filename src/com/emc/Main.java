package com.emc;

import com.emc.model.Artist;
import com.emc.model.DataSource;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        DataSource dataSource = new DataSource();

        if(!dataSource.open()){
            System.out.println("can't open datasource");
            return;
        }

        List<Artist> artists = dataSource.queryArtist();
        printArtistList(artists);

        dataSource.close();
    }

    public static void printArtistList(List<Artist> artists) {
        if (artists == null || artists.isEmpty()) {
            System.out.println("no artist");
        } else {
            for (Artist artist: artists) {
                System.out.println("ID: " +artist.getId()+ " | Name: " +artist.getName());
            }
        }
    }
}
