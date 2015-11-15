from hdfs import TokenClient
from pymongo import MongoClient
from bson.json_util import dumps
import click

@click.command()
@click.option('--hdfsconnection', help='Directory on hdfs to output to')
@click.option('--hdfs', help='Directory on hdfs to output to')
@click.option('--filename', help='filename to save to')
@click.option('--username', help='The owner of the file')
@click.option('--connection', help='URL to connect to mongodb')
@click.option('--collection', help='Collection to export')
@click.option('--database', help='Database to export from')
def dump(connection, collection, database, hdfs, filename, username, hdfsconnection):
    client = TokenClient(hdfsconnection, username, root=hdfs)
    length = getCollection(connection,database, collection).find().count()
    with client.write(filename, encoding='utf-8') as writer:
        with click.progressbar(getCollection(connection,database, collection).find(), length=length, label='Writing collection: '+ collection+" to HDFS: "+ filename) as bar:
            for j in bar:
                writer.write(dumps(j))

def getCollection(connection, database, collection):
    client = MongoClient(connection)
    db = client[database]
    return db[collection]

if __name__ == '__main__':
    dump()