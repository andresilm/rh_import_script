import json
import logging
import sys
import datetime
import requests


from datasources.models import DataSource

from DatetimeUtils import DatetimeUtils
from UpdateSessionJobsPool import UpdateSessionJobsPool

logging.basicConfig(filename='import_script.log',
                    filemode='w',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

import_jobs = UpdateSessionJobsPool()


def main(args):
    if len(args) < 3:
        print("Usage example: 28-02-2018 01-03-2018")
        return

    print("PROCESS STARTED")

    date_start = DatetimeUtils.string_to_date(args[1])
    start_datetime = DatetimeUtils.date_to_datetime(date_start)
    final_timedate = DatetimeUtils.date_to_datetime(DatetimeUtils.string_to_date(args[2]))

    start_date = start_datetime.date()
    end_date = final_timedate.date()
    day_count = (end_date - start_date).days + 1
    date_from = start_date
    all_dates_to_process = [d for d in (start_date + datetime.timedelta(n + 1) for n in range(day_count)) if d <= end_date]

    import_jobs.set_relaunch_function(count_entries_in_databases)

    for date_to in all_dates_to_process:
        print('BEGIN CHECKING: ' + str(date_from) + ' to ' + str(date_to))
        count_entries_in_databases(date_from, date_to)
        print('END CHECKING: ' + str(date_from) + ' to ' + str(date_to))
        date_from = date_to

    while import_jobs.has_pending_items():
        pass

    print("PROCESS FINISHED")
    print("Total dates: " + str(len(all_dates_to_process)))
    print("Total reimports: " + str(import_jobs.total_imports))

def count_difference(date_from):
    elastic_count = count_data_elastic(DatetimeUtils.date_to_elastic_str(date_from))
    oracle_count = count_data_oracle(DatetimeUtils.date_to_datetime(date_from))


    print('oracle_count: ' + str(oracle_count))
    print('elastic_count: ' + str(elastic_count))

    return elastic_count - oracle_count

def count_entries_in_databases(date_from, date_to):
    print('launch_count_job ' + str(date_from) + ' ' + str(date_to))

    diff = count_difference(date_from)

    if diff < 0:
        print('Must reimport data for date ' + str(date_from))
        import_jobs.enqueue_job(date_from, date_to)
    elif diff > 0:
        print('Will not reimport data but elastic count was greater')
    else:
        print('Everything ok on date ' + str(date_from))

def count_data_oracle(datetime_start):
    con = DataSource.objects.get(name='DNM - OSTOLBDA').connector.connect()

    datetime_end = datetime_start + datetime.timedelta(days=1, seconds=-1)
    datetime_end_str = DatetimeUtils.datetime_to_oracle_str(datetime_end)
    datetime_start_str = DatetimeUtils.datetime_to_oracle_str(datetime_start)

    print('count_data_oracle from ' + datetime_start_str + ' to ' + datetime_end_str)

    bind = {'fecha_desde': datetime_start_str, 'fecha_hasta': datetime_end_str}
    query = "SELECT count(rid_idtran_ol_sicam) FROM bda.transitos WHERE fecha_insert BETWEEN TO_DATE(:fecha_desde, 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE(:fecha_hasta, 'DD/MM/YYYY HH24:MI:SS')"

    data = con.execute(query, bind).fetchall()
    print(data)
    return data[0][0] # [(result)]

def count_data_elastic(date_str):
    print('count_data_elastic ' + date_str)
    count = -1
    url = 'http://sma-prod-elasticsearch.apps.migraciones.cloud/crossing_index/_search?scroll=1m'
    json_str = '{"from":0,"query":{"constant_score":{"filter":{"bool":{"must":[{"term":{"_source_last_update":"'+ date_str +'"}},{"query_string":{"query":"(source:SICAM)"}}]}}}}}'

    response = requests.post(url, data=json_str, headers={"content-type":"application/json", "Authorization": "Basic YWRtaW46Y2hhbmdlbWU="})
    print('elastic response code = ' + str(response.status_code))

    if requests.codes.ok == response.status_code:
        results = response.json()
        if results['hits']:
            count = results['hits']['total']

    return count


if  __name__ =='__main__':
    main(['', '28-02-2018','01-03-2018'])
