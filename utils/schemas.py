from pyspark.sql.types import LongType as Long, DecimalType as Dec, StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
songSchema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dec(10,5)),
    Fld("artist_location",Str()),
    Fld("artist_longitude", Dec(10,5)),
    Fld("artist_name",Str()),
    Fld("duration",Dec(10,5)),
    Fld("num_songs",Int()),
    Fld("song_id", Str()),
    Fld("title",Str()),
    Fld("year",Int()),
])

logSchema = R([
    Fld("artist",Str()),
    Fld("auth", Str()),
    Fld("firstName",Str()),
    Fld("gender", Str()),
    Fld("itemInSession",Int()),
    Fld("lastName",Str()),
    Fld("length",Dec(10,5)),
    Fld("level", Str()),
    Fld("location",Str()),
    Fld("method",Str()),
    Fld("page",Str()),
    Fld("registration", Dbl()),
    Fld("sessionId",Int()),
    Fld("song", Str()),
    Fld("status",Str()),
    Fld("ts",Long()),
    Fld("userAgent",Str()),
    Fld("userId", Str()),
])