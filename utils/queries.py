from utils.querybuilder import QueryBuilder
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

# tables paths
songs = config['S3']['SONGS']
songplays = config['S3']['SONGPLAYS']
time = config['S3']['TIME']
users = config['S3']['USERS']
artists = config['S3']['ARTISTS']


def popular_songs(sc):
    query = QueryBuilder(sc).name('popular_songs').add_table('songs', songs, 'parquet') \
                            .add_table('songplays', songplays, 'parquet') \
                            .query("""SELECT s.title, count(sp.song_id) 
                                        FROM songplays sp JOIN songs s 
                                        ON (sp.song_id = s.song_id)
                                        GROUP BY s.title, sp.song_id
                                        ORDER BY count(sp.song_id) DESC
                                        LIMIT 5""")

    return query


def busiest_wday(sc):
    query = QueryBuilder(sc).name('busiest_wday').add_table('time', time, 'parquet') \
                            .add_table('songplays', songplays, 'parquet') \
                            .query("""select count(t.weekday) as count_weekday, t.weekday
                                        from songplays sp, time t
                                        where sp.start_time = t.start_time
                                        group by t.weekday 
                                        limit 3""")

    return query


def busiest_hour(sc):
    query = QueryBuilder(sc).name('busiest_wday').add_table('time', time, 'parquet') \
                            .add_table('songplays', songplays, 'parquet') \
                            .query("""select count(t.hour) as plays, t.hour as hour
                                    from songplays sp, time t
                                    where sp.start_time = t.start_time
                                    and t.weekday = 6
                                    group by t.hour
                                    order by plays desc
                                    limit 5""")

    return query


def busiest_state_friday(sc):
    query = QueryBuilder(sc).name('busiest_state_friday').add_table('time', time, 'parquet') \
                            .add_table('songplays', songplays, 'parquet') \
                            .query("""select right(location, 2) as state, count(right(location, 2)) as plays
                                        from songplays sp, time t
                                        where sp.start_time = t.start_time and t.weekday = 5
                                        group by state
                                        order by plays desc
                                        limit 5""")

    return query


# export all queries here
analytical_queries = [popular_songs, busiest_wday, busiest_hour, busiest_state_friday]
