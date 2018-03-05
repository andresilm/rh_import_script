import datetime

class DatetimeUtils:
    def date_to_elastic_str(date):
        return date.strftime('%Y-%m-%d')

    def string_to_date(date_str):
        return datetime.datetime.strptime(date_str, '%d-%m-%Y')

    def date_to_datetime(date):
        return datetime.datetime(date.year, date.month, date.day)

    def datetime_to_oracle_str(datetime):
        return datetime.strftime('%d/%m/%Y %H:%M:%S')