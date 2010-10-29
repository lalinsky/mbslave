\unset ON_ERROR_STOP

CREATE INDEX l_album_album_idx_link1 ON l_album_album (link1);
CREATE INDEX l_album_artist_idx_link1 ON l_album_artist (link1);
CREATE INDEX l_album_label_idx_link1 ON l_album_label (link1);
CREATE INDEX l_album_track_idx_link1 ON l_album_track (link1);
CREATE INDEX l_album_url_idx_link1 ON l_album_url (link1);
CREATE INDEX l_artist_artist_idx_link1 ON l_artist_artist (link1);
CREATE INDEX l_artist_label_idx_link1 ON l_artist_label (link1);
CREATE INDEX l_artist_track_idx_link1 ON l_artist_track (link1);
CREATE INDEX l_artist_url_idx_link1 ON l_artist_url (link1);
CREATE INDEX l_label_label_idx_link1 ON l_label_label (link1);
CREATE INDEX l_label_track_idx_link1 ON l_label_track (link1);
CREATE INDEX l_label_url_idx_link1 ON l_label_url (link1);
CREATE INDEX l_track_track_idx_link1 ON l_track_track (link1);
CREATE INDEX l_track_url_idx_link1 ON l_track_url (link1);
CREATE INDEX l_url_url_idx_link1 ON l_url_url (link1);

-- vi: set ts=4 sw=4 et :
