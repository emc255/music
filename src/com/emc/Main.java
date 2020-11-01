package com.emc;

import com.emc.model.Artist;
import com.emc.model.ArtistList;
import com.emc.model.DataSource;
import com.emc.model.OrderBy;


import java.util.List;

public class Main {

    public static void main(String[] args) {
        DataSource dataSource = new DataSource();

        if(!dataSource.open()){
            System.out.println("can't open datasource");
            return;
        }

        List<Artist> artists = dataSource.queryArtist(OrderBy.ASCENDING);
       // printArtistList(artists);
            List<String> albumListByArtist = (dataSource.queryAlbumsForArtist("Deep Purple", OrderBy.ASCENDING));
        printListString(albumListByArtist);

        List<ArtistList> artistLists = dataSource.queryArtistsForSong("Go Your Own Way",OrderBy.DESCENDING);
        printArtistList2(artistLists);
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

    public static void printArtistList2(List<ArtistList> artistLists) {
        if (artistLists == null || artistLists.isEmpty()) {
            System.out.println("no artist");
        } else {
            for (ArtistList artistList: artistLists) {
                System.out.println("Name: " +artistList.getArtist()+ " | Album: " +artistList.getAlbum()+ " | Track: " +artistList.getTrack()+ " | Title: " +artistList.getTitle());
            }
        }
    }

    public static void printListString(List<String> arrList) {
        if (arrList == null || arrList.isEmpty()) {
            System.out.println("List of String is empty");
        } else {
            for (String element: arrList) {
                System.out.println(element);
            }
        }
    }

}
