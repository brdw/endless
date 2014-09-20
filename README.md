Endless
=======

Endless is a generic and dynamic REST backend for NoSQL databases with the goal of reducing the need for server side code through convention. Most JS code typically interests against an api server providing data via some REST incarnate. The Endless project was born out of the realization that these url concepts map naturally so NoSQL key/val stores and Endless exploits this. With a few simple conventions you can dynamically create your backend from your JS code in a scalable way.

Currently Endless isn't suitable for production as it provides no data security, however Endless makes for an excellent hacking and prototyping platform when your data is tighly coupled to your app and installed in a secure location. We hope to change this in coming versions.


# Installation

Currently we are only supporting Cassandra 2.* and officially working with 2.0.9. The default installation of Cassandra out of the box works fine.

To configure cassandra for Endless we have provide a fabfile that creates the keyspace and table where all data is stored.

$ fab hosts:localhost install

Installation of Endless itself is a simple command provided you don't have trouble compiling c libs

$ python setup.py install

Assuming everything is running on localhost we have provided a simple example of configuring and starting the server in examples/simple_server.py

$ cd examples

$ python simple_server.py 

INFO:werkzeug: * Running on http://127.0.0.1:5000/

Pointing your browser to http://localhost:5000/ should then show you the welcome json to let you know everything is working

# Adding data

Adding data is very straight forward. We highly recommend having a json viewing plugin such as JSONView for Chrome installed. 

Endless only lets you add, update and remove objects, and all objects must be in a collection. 
Let's create two users in a users collection with curl. The url becomes a key for that object.

## Creating Objects and making Root collections
$ curl -X POST http://localhost:5000/users/brad/ -H "Content-type: application/json" -d '{"name":"brad", "title":"engineer"}'
$ curl -X POST http://localhost:5000/users/adrian/ -H "Content-type: application/json" -d '{"name":"adrian", "title":"mobileengineer"}'

If your data is echoed back to you, it's stored and immediately accessible via those same urls in your browser. Since these are 'top level' collections they are special and play a special part in the sharding scheme in the data store. We call these 'Root Collections' and they are not iterable. However once an object is in a 'Root Collection' it can have sub collections that are iterable. So let's setup some friends.

$ curl -X POST http://localhost:5000/users/brad/friends/adrian -H "Content-type: application/json" -d '{"name":"adrian"}'
$ curl -X POST http://localhost:5000/users/brad/friends/sarah -H "Content-type: application/json" -d '{"name":"sarah"}'

Now these urls are special because they are Sub Collections. Going to those urls without trailing slashes indicates to Endless these are Objects just like before. However because they have a common url component /friends/ we can now iterate on this special friends Collection via http://localhost:5000/users/brad/friends/ and we will see that both Adrian and Sarah are returned. These Sub Collections are iterable and support iteration and key slicing. Let's add a few more and do some iteration and slicing.

$ curl -X POST http://localhost:5000/users/brad/friends/dave -H "Content-type: application/json" -d '{"name":"dave"}'
$ curl -X POST http://localhost:5000/users/brad/friends/rachael -H "Content-type: application/json" -d '{"name":"rachael"}'

## Page Size
You can pass a page_size param to any Sub Collection url to start paging. Larger pages are good for bulk data processing.
http://localhost:5000/users/brad/friends/?page_size=1

## Slices
Sub collections supporting slicing via params: gt, gte, lt, and lte (greater than, greather than equal, less than, and less than equal) params
http://localhost:5000/users/brad/friends/?gt=adrian

 