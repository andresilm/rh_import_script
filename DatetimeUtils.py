import datetime
import pytz

class DatetimeUtils:
    def date_to_str(date):
        return date.strftime('%d-%m-%Y')

    def string_to_date(date_str):
        return datetime.datetime.strptime(date_str, '%d-%m-%Y', tzinfo=pytz.UTC)

    def date_to_datetime(date):
        return datetime.datetime(date.year, date.month, date.day, tzinfo=pytz.UTC)

    def datetime_to_str(datetime):
        return datetime.strftime('%d-%m-%Y %H:%M:%S')