Endless
=======

Endless is a generic and dynamic REST backend for NoSQL databases with the goal of reducing the need for server side code through convention. Most JS code typically interests against an api server providing data via some REST incarnate. The Endless project was born out of the realization that these url concepts map naturally so NoSQL key/val stores and Endless exploits this. With a few simple conventions you can dynamically create your backend from your JS code in a scalable way.

Currently Endless isn't suitable for production as it provides no data security, however Endless makes for an excellent hacking and prototyping platform when your data is tighly coupled to your app and installed in a secure location. We hope to change this in coming versions.


# Installation

Currently we are only supporting Cassandra 2.* and officially working with 2.0.9. The default installation of Cassandra out of the box works fine.

To configure cassandra for Endless we have provide a fabfile that creates the keyspace and table where all data is stored.

> fab hosts:localhost install

Installation of Endless itself is a simple command provided you don't have trouble compiling c libs

> python setup.py install

Assuming everything is running on localhost we have provided a simple example of configuring and starting the server in examples/simple_server.py

> cd examples
> python simple_server.py 
INFO:werkzeug: * Running on http://127.0.0.1:5000/

Pointing your browser to http://localhost:5000/ should then show you the welcome json to let you know everything is working

# Adding data

Endless exploits the REST concepts of collections and objects. Let's create a new user and put them in the /users collections. We do this via POST, passing a json header, and then the json data as a string.
 