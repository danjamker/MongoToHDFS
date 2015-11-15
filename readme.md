## MongoToHDFS
A simple tool to dump a collection from Mongodb into a file on HDFS. Each line is the file is a json dump of a record.

# Dependencies
hdf

pymongo 

click

# Example
MongoToHDFS.py --connection mongodb://username:password@url:port --collection c --database d --hdfsconnection http://url:port --hdfs /user/danjamker --filename outputgoog.json --username danjamker