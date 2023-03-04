from google.cloud import bigquery
import datetime as dt
import pendulum
import dateutil
import google
import re

client = bigquery.Client()
project_id=''
#credential = client.cre
#project_id = credential.project_id


def split_table_info(table_name):
    if ':' not in table_name:
        if len(table_name.split('.')) != 3:
            raise ValueError('Invalid table_name: {}'.format(table_name))
        table_name = table_name.replace('.', ':', 1)
    else:
        if len(table_name.split(':')[1].split('.')) != 2:
            raise ValueError('Invalid table_name: {}'.format(table_name))

    table_proj = table_name.split(':')[0]
    table_ds = table_name.split(':')[1].split('.')[0]
    table_nme = table_name.split(':')[1].split('.')[1].split('$')[0]
    if len(table_name.split(':')[1].split('.')[1].split('$')) == 1:
        partition_id = None
    else:
        partition_id = table_name.split(':')[1].split('.')[1].split('$')[1]
    return table_proj, table_ds, table_nme, partition_id


def parition_lst(table_name):
    client = bigquery.Client(project_id)
    table_proj, table_ds, table_nme, _ = split_table_info(table_name)
    table_ref = client.dataset(table_ds, table_proj).table(table_nme)
    return [x for x in client.list_partitions(table_ref) if re.match('\d{8}', x)]


def delete_partition(table_name, partition_time=None):
    table_proj, table_ds, table_nme, _ = split_table_info(table_name)
    if partition_time is not None:
        partition_time = partition_time.replace('-', '')  # remove '-'
        table_nme = '{}${}'.format(table_nme, partition_time)
    client = bigquery.Client(project_id)

    table_ref = client.dataset(table_ds, table_proj).table(table_nme)
    table_full_name = table_name
    if partition_time is not None:
        table_full_name = '{}${}'.format(table_full_name, partition_time)
    print('Deleting table: {}'.format(table_full_name))
    try:
        client.delete_table(table_ref)
        print('Table: {} deleted'.format(table_full_name))
    except google.api_core.exceptions.NotFound:
        print('Table: {} not found'.format(table_full_name))


def partition_life_cycle(table_name, life_cycle = None, top_x = None, base_date_str = None):
    '''
    Delete partition from {table_name} with partition < base_date - life_cycle in days
    '''
    if life_cycle is not None:
        base_date = dateutil.parser.parse(base_date_str) if base_date_str is not None else \
            dt.datetime.utcnow().astimezone(tz=pendulum.timezone('America/Chicago'))
        cutoff = (base_date - dt.timedelta(days = life_cycle)).strftime('%Y%m%d')
    if (top_x is not None and life_cycle is not None) or (top_x is None and life_cycle is None):
        raise ValueError('Invalid parameter life_cycle and top_x')
    partitions = parition_lst(table_name)
    #print(partitions.__str__())
    if top_x is not None:
        top_partition = sorted(partitions)[::-1][:top_x]
        delete_partition_lst = list(set(partitions) - set(top_partition))
    if life_cycle is not None:
        delete_partition_lst = [x for x in partitions if x < cutoff]
    print(sorted(delete_partition_lst).__str__())
    for part in sorted(delete_partition_lst):
        delete_partition(table_name, part)

if __name__ == '__main__':
    table_name = 'X'
    top_x = 180
    partition_life_cycle(table_name,None,top_x)
    print("completed main method")