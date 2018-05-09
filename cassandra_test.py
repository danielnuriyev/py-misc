

from cassandra.cluster import Cluster

if __name__ == '__main__':

    cluster = Cluster()
    session = cluster.connect()

    keyspace = '''CREATE  KEYSPACE test  WITH REPLICATION = { 
                           'class' : 'SimpleStrategy', 'replication_factor' : 1 
                    }'''
    #session.execute(keyspace)
    session.set_keyspace('test')

    drop = '''DROP TABLE test_ts'''
    #session.execute(drop)

    table = '''CREATE TABLE test_ts (
                    k varchar,
                    t float,
                    v double,
                    PRIMARY KEY (k, t)
                    
                )'''
    #session.execute(table)

    insert = 'INSERT INTO test_ts (k, v, t) VALUES (%s, %s, %s)'
    session.execute(insert, ('test', 0, 0))
    session.execute(insert, ('test', 1, 1))
    session.execute(insert, ('test', 2, 2))

    select = "SELECT * FROM test_ts WHERE k=%s"
    rows = session.execute(select, ['test'])
    for r in rows:
        print(r)

    session.shutdown()
