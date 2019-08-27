import configparser
import psycopg2

def get_results(cur, conn):

    fact_songplay_count = ("""
        SELECT COUNT(*) FROM fact_songplay
    """)

    dim_user_count = ("""
        SELECT COUNT(*) FROM dim_user
    """)

    dim_song_count = ("""
        SELECT COUNT(*) FROM dim_song
    """)

    dim_artist_count = ("""
        SELECT COUNT(*) FROM dim_artist
    """)

    dim_time_count = ("""
        SELECT COUNT(*) FROM dim_time
    """)
    
    staging_events_count = ("""
        SELECT COUNT(*) FROM staging_events
    """)
    
    staging_songs_count = ("""
        SELECT COUNT(*) FROM staging_songs
    """)
    
    queries = [staging_events_count,staging_songs_count,fact_songplay_count,dim_user_count,dim_song_count,dim_artist_count,dim_time_count]

    for query in queries:
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print(row)


def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    get_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()