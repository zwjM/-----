
import pandas
import numpy
import datetime as dt
from tool_kit.connect_to_database import connect_db

pd = pandas
np = numpy
datetime = dt.datetime
timedelta = dt.timedelta
time = dt.time
client = connect_db(None, name='weimingliang', pwd='weimingliang')
db_zcs = client['zcs']
