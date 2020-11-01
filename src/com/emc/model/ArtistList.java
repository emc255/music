package com.emc.model;

public class ArtistList {
    private final String artist;
    private final String album;
    private final int track;
    private final String title;

    public ArtistList(String artist, String album, int track, String title) {
        this.artist = artist;
        this.album = album;
        this.track = track;
        this.title = title;
    }

    public String getArtist() {
        return artist;
    }

    public String getAlbum() {
        return album;
    }

    public int getTrack() {
        return track;
    }

    public String getTitle() {
        return title;
    }
}
