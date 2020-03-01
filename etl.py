import glob
import os
from typing import List

import pandas as pd
import psycopg2
from numpy import int64
from psycopg2.extensions import connection, cursor

from db_values import HOST, DBNAME, USER, PASSWORD
from sql_queries import (SONG_TABLE_INSERT, ARTIST_TABLE_INSERT,
                         TIME_TABLE_INSERT, USER_TABLE_INSERT, SONG_SELECT,
                         SONGPLAY_TABLE_INSERT)

# To avoid `psycopg2.ProgrammingError: can't adapt type 'numpy.int64'` when
# executing the INSERT command:
psycopg2.extensions.register_adapter(int64, psycopg2._psycopg.AsIs)


def process_song_file(cur: cursor,
                      filepath: str,
                      song_table_insert: str = SONG_TABLE_INSERT,
                      artist_table_insert: str = ARTIST_TABLE_INSERT):
    """
    Read JSON file and insert data into songs and artists dimensional tables.

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Database cursor.
    filepath : str
        Path of the JSON file.
    song_table_insert : str, optional
        INSERT statement.
    artist_table_insert : str, optional
        INSERT statement.

    Returns
    -------
    None
    """
    # Read song file
    df = pd.read_json(filepath, lines=True)

    # All columns will be used below, except "num_songs", which seems to be
    # always equal to 1.

    # Insert song record
    song_data = (df
                 .loc[0, ['song_id', 'title', 'artist_id', 'year', 'duration']]
                 .values.tolist())
    cur.execute(song_table_insert, song_data)

    # Insert artist record
    artist_data = (df
                   .loc[0, ['artist_id', 'artist_name', 'artist_location',
                            'artist_latitude', 'artist_longitude']]
                   .values.tolist())
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath: str):
    """
    Read JSON file and insert data into time and users dimensional tables, and
    into songplays fact table.

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Database cursor.
    filepath : str
        Path of the JSON file.

    Returns
    -------
    None
    """
    # Open log file
    df = pd.read_json(filepath, lines=True)

    # Columns that are not used:
    # - "auth" (e.g., "Logged In")
    # - "itemInSession" (e.g., 0, 1, 2, ...)
    # - "method" (e.g., "GET", "PUT")
    # - "registration" (e.g., 1540919166796)
    # - "status" (e.g., 200)

    # Filter by NextSong action
    df = df.loc[df['page'] == 'NextSong', :]

    # Convert timestamp column to datetime
    t: pd.Series = pd.to_datetime(df['ts'], utc=True, unit='ms')

    # Insert time data records
    time_data = (t, t.dt.year, t.dt.month, t.dt.day, t.dt.hour,
                 t.dt.weekofyear, t.dt.weekday)
    column_labels = ('timestamp', 'year', 'month', 'day', 'hour', 'weekofyear',
                     'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for _, row in time_df.iterrows():
        cur.execute(TIME_TABLE_INSERT, row)

    # Load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Insert user records
    for _, row in user_df.iterrows():
        cur.execute(USER_TABLE_INSERT, row)

    # Insert songplay records
    for index, row in df.iterrows():

        # Get song_id and artist_id from song and artist tables
        cur.execute(SONG_SELECT, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = ([t[index], songid, artistid] +
                         row[['userId', 'sessionId', 'level', 'location',
                              'userAgent']].values.tolist())
        cur.execute(SONGPLAY_TABLE_INSERT, songplay_data)


def get_files(filepath: str, pattern: str = '*.json') -> List[str]:
    """
    Get all files matching pattern in directory.

    Parameters
    ----------
    filepath : str
        Path with files possibly under sub-folders.
    pattern : str, optional
        Pattern to search in file names.

    Returns
    -------
    list[str]
        List of full file names.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, pattern))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def process_data(cur: cursor, filepath: str, func: callable) -> None:
    """
    Run `func` on all files under `filepath`.

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Database cursor.
    filepath : str
        Path with files possibly under sub-folders.
    func : callable
        Function.

    Returns
    -------
    None
    """

    # Get all files matching extension in directory
    all_files = get_files(filepath)

    # Get total number of files found
    num_files = len(all_files)
    print('{:,d} files found in "{}"'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, start=1):
        func(cur, datafile)
        print('{:03d}/{:03d} files processed'.format(i, num_files))


def main():
    conn = psycopg2.connect(host=HOST, dbname=DBNAME, user=USER,
                            password=PASSWORD)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, filepath='data/song_data', func=process_song_file)
    process_data(cur, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
