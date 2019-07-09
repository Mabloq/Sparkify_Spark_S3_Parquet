##custom year partitioner
from pyspark.sql.types import StringType as Str
from pyspark.sql.functions import udf


@udf(Str())
def year_part_udf(year):
    if year == 0:
        return '0'
    elif year <= 1979:
        return '1920-1979'
    elif year <= 1999:
        return '1980-1999'
    elif year <= 2009:
        return '2000-2009'
    else:
        return '2010-Present'
