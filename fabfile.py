__author__ = 'bwillard'


from fabric.api import local, settings, abort, run, cd, env, lcd, put, hosts, task, prefix, sudo
from fabric.contrib.console import confirm


_tmp_file = '/tmp/endless_tmp.txt'

_create_keyspace = '''
    CREATE KEYSPACE endless
    WITH replication = {'class':'%s', 'replication_factor': %s};
'''

_create_store = '''
    use %s;

    create table datastore (
        collection_id text,
        item_id text,
        key text,
        value text,
        primary key (collection_id, item_id, key)
    ) WITH COMPACT STORAGE;
'''

def create_keyspace(keyspace, replicate_strategy='SimpleStrategy', replication_factor=3):

    create_cmd = _create_keyspace % (replicate_strategy, replication_factor)

    with open(_tmp_file, 'w') as f:
        f.write(create_cmd)
    remote_path = '/tmp/endless_keyspace.txt'

    put(_tmp_file, remote_path)

    run('cqlsh -f %s' % remote_path)


def create_store(keyspace):
    store_cmd = _create_store % keyspace

    with open(_tmp_file, 'w') as f:
        f.write(store_cmd)
    remote_path = '/tmp/endless_store.txt'

    put(_tmp_file, remote_path)

    run('cqlsh -f %s' % remote_path)


@task
def hosts(*hosts):
    env.hosts = hosts


@task
def install(keyspace='endless', replicate_strategy='SimpleStrategy', replication_factor=3):
    create_keyspace(keyspace, replicate_strategy=replicate_strategy, replication_factor=replication_factor)
    create_store(keyspace)

    


