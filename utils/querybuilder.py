from time import time
from datetime import datetime
from functools import wraps


class GenerativeBase(object):
    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s


def _generative(func):
    @wraps(func)
    def decorator(self, *args, **kw):
        self = self._generate()
        func(self, *args, **kw)
        return self
    return decorator


class QueryBuilder(GenerativeBase):
    def __init__(self, sc):
        self.sc = sc
        self.query_name = "nameless-".format(datetime.now())
        self.query_string = None
        self.destination = None
        self.duration = None
        self.result = None

    @_generative
    def name(self, n):
        """
        Initializes name for query

        :param n: name for query
        """
        self.query_name = n

    @_generative
    def add_table(self, name, path, file_type):
        """
        reads file into pyspark.sql.DataFrame, then creates a
        pyspark.sql.DataFrame.createOrReplaceTempView in our spark session

        :param name: name to be used for tempview
        :type name: str

        :param path: path of file being read from datalake (s3,json,etc)
        :type path: str

        :param file_type: specifies spark dataframe reader to use (json, csv, parquet)
        :type file_type: str

        """
        ### dont load the database again if it alread has a temp view
        try:
            self.sc.read.table(name)
            print('========== ========== Table Exists ======== ===========')
        except:
            self.sc.read.format(file_type).load(path).createOrReplaceTempView(name)

    @_generative
    def query(self, query_string):
        """
        saves query to self.query

        :param query_string: the hive query for spark to execute
        :return:
        """
        self.query_string = query_string

    @_generative
    def execute(self):
        """
        Executes the query, stores it self.result marking the time it took to get result
        in self.duration

        """

        start = time()
        print("==============  Executing  ** {} query **  =================".format(self.query_name))

        self.result = self.sc.sql(self.query_string)

        query_time = time() - start
        self.duration = query_time

    def save(self, destination):
        """
        Saves query result to destination path

        :param destination: path to storage of query results
        """
        self.result.write.mode('overwrite').format('csv').save(destination)


