CREATE DATABASE IF NOT EXISTS musicproject;

USE musicproject;
DROP TABLE IF EXISTS top_10_stations;
CREATE TABLE IF NOT EXISTS top_10_stations
(
station_id VARCHAR(50),
total_songs_played_liked_by_unique_user INT
);
DROP TABLE IF EXISTS user_type_duration;
CREATE TABLE IF NOT EXISTS user_type_duration
(
user_type VARCHAR(50),
total_duration_played DOUBLE
);
DROP TABLE IF EXISTS connected_artists;
CREATE TABLE IF NOT EXISTS connected_artists
(
artist_id VARCHAR(50),
unique_followers INT
);
DROP TABLE IF EXISTS top_10_songs_maxrevenue;
CREATE TABLE IF NOT EXISTS top_10_songs_maxrevenue
(
song_id VARCHAR(50),
total_duration_played_in_minutes DOUBLE
);
DROP TABLE IF EXISTS top_10_unsubscribed_users;
CREATE TABLE IF NOT EXISTS top_10_unsubscribed_users
(
user_id VARCHAR(50),
total_duration_played DOUBLE
);