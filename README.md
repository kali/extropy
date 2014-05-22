# extropy

Language- and Framework-agnostic declarative denormalization for MongoDB.

## What's the point

In order to get the most of a MongoDB database, developpers have to jump through the painful hoops of denormalization.
extropy aims at getting this complexity away.

extropy main component is a MongoDB proxy. All interactions (at least the ones performing write ops) from the
applications must go through the proxy. In a sharded setup, a extropy proxy must front each instance of mongos. In a
standalone setup, a single proxy will front the mongod. 

Once this setup is performed, extropy will handle all ancilliary writes for each write op. It also supports adding
rules on pre-existing data.

extropy is coded in scala, but strictly no scala knowledge is required for using it.

## Example

Imagine a minimalist blog engine. It has two collections: users, and posts.
Posts containts comments as embedded subdocuments.

A fully-normalized data set could look like that (note that I have omited many fields... like the actual text for the posts):

````javascript
> db.posts.find()
{ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "liz" }
{ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz", "comments" : [ { "_id" : "comment1", "authorId" : "jack" } ]  }

> db.users.find()
{ "_id" : "jack", "name" : "John Francis \"Jack\" Donaghy" }
{ "_id" : "liz", "name" : "Elizabeth Lemon" }
````

In order to obtain good read performance for /users/:id and /posts/:id, or case insensitive searchability, 
or better indexability we may need some denormalization to appear in the data:
* post must contain the author name
* comment must contain the author name
* post must contain comment count
* user must contain post count for the user
* user must contain comment count for the user
* post must contain a case insensitive version of its title

The resulting data should look like this:

````javascript
> db.posts.find()
{ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "liz", "searchableTitle" : "title for post 1",
    "authorName" : "Elizabeth Lemon", "commentCount" : 0 }
{ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz",
    "comments" : [ { "_id" : "comment1", "authorId" : "jack", "authorName" : "John Francis \"Jack\" Donaghy" } ],
    "searchableTitle" : "title for post 2", "authorName" : "Elizabeth Lemon", "commentCount" : 1 }
> db.users.find()
{ "_id" : "jack", "name" : "John Francis \"Jack\" Donaghy", "postCount" : 0, "commentCount" : 1 }
{ "_id" : "liz", "name" : "Elizabeth Lemon", "postCount" : 2, "commentCount" : 0 }
````

The purpose of extropy is to get a declarative way to manage these denormalized fields: let's define some "rules":

```javascript
{ "rule" : { "from" : "blog.posts", "follow" : "authorId", "to" : "blog.users" }, "authorName" : "name" }
{ "rule" : { "from" : "blog.posts.comments", "follow" : "authorId", "to" : "blog.users" }, "authorName" : "name" }
{ "rule" : { "from" : "blog.posts", "unwind" : "comments" }, "commentCount" : { "count" : true } }
{ "rule" : { "from" : "blog.users", "search" : "blog.posts", "by" : "authorId" }, "postCount" : { "count" : true } }
{ "rule" : { "from" : "blog.users", "search" : "blog.posts.comments", "by" : "authorId" }, "commentCount" : { "count" : true } }
{ "rule" : { "from" : "blog.posts", "same" : "blog.posts" }, "searchableTitle" : { "mvel" : "title.toLowerCase()", "using" : [ "title" ] } }
```

Each rule definition must start with a "rule" field. Its value describe how documents are "tied", from the document
containing the denormalization, to the source-of-authority document. Four "ties" are actually supported:
* follow: for N-to-1 situation,
* search: just the opposite,
* unwind: like the $unwind in the aggregation framework, dig down in an array of subdocuments,
* same: stay in the same document.

Then comes one or several expressions describing what field are to be denormalized and how. A string value is
a plain copy, while objects denotes more complex operations.

## Run the example

You need java in your $PATH. I use version jdk 8, but 6 and 7 should be ok.
You'll also need a MongoDB server you can safely play with, and I'll be using the mongo client to demounstrate the
main features.

Compile and run the proxy:

````shell
git clone https://github.com/kali/extropy.git
cd extropy
./sbt "agent/run --help"
````

If you're not a regular sbt or maven user, it may take a while, but you should evantually, you should get a list
of options.

```
./sbt "agent/run --listen localhost:27000 --payload localhost:27017"
```

payload must target the mongodb where the actual data lives.
extropy will generate a database in it (called... "extropy") to store rules definitions and handle communication
between the various proxy and workers of your system.
You can use --extropy to specify an alternate location (it can be an entirely separate server)

From this point, you should always connect to mongodb through the proxy. Let's start by populating our blog database:

````
% mongo localhost:27000/blog
db.posts.save({ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "liz" })
db.posts.save({ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz", "comments" : [ { "_id" : "comment1", "authorId" : "jack" } ]  })
db.users.save({ "_id" : "jack", "name" : "John Francis \"Jack\" Donaghy" })
db.users.save({ "_id" : "liz", "name" : "Elizabeth Lemon" })
````

So far so good. The proxy has no rules defined yet, so it does nothing out of the ordinary.
If you find() the documents, they are just as you are expecting them.

Let's define out rule. We need to call a command on extropy. The syntax may look a bit convoluted, but it mimicks
the way mongodb does "runCommand" under the hood.

````
db.$extropy.findOne({ "addRule": { "rule" : { "from" : "blog.posts", "follow" : "authorId", "to" : "blog.users" }, "authorName" : "name" }} )
````

The proxy output may spew a few lines to state that it actually performed a Sync on a new rule, and you can verify
the result:

````
db.posts.find()
{ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "liz", "authorName" : "Elizabeth Lemon" }
{ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz", "comments" : [ { "_id" : "comment1", "authorId" : "jack" } ], "authorName" : "Elizabeth Lemon" }
````

Now you can try and perform updates: 
````
db.posts.update({_id: "post1"}, { "$set" : { authorId: "jack" }})
db.users.update({_id: "liz"}, { "$set" : { name: "Liz Lemon" }})
{ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "liz", "authorName" : "Elizabeth Lemon" }
{ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz", "comments" : [ { "_id" : "comment1", "authorId" : "jack" } ], "authorName" : "Elizabeth Lemon" }
db.posts.find()
{ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "jack", "authorName" : "John Francis \"Jack\" Donaghy" }
{ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz", "comments" : [ { "_id" : "comment1", "authorId" : "jack" } ], "authorName" : "Liz Lemon" }
````

Both changes (authorId on post1 and author full name for liz) have been entirely propagated.

## Current status, roadmap

extropy is neither feature complete nor production ready. It's a proof-of-concept.

While I've done my best to get a good unit and integration test converage, and not code too stupid things, expect:
- gaping holes in the feature set (see below)
- zero performance profiling or optimisation
- no existing test coverage for adverse conditions.

Here is a non-exhaustive list of well-defined (at least in my mind) features in the todo list:
* rules
    * support more tie types: 1-to-1 embedded, 1-to-1 by reference [easy]
    * support N-to-N ties (developper maintains follower array, extropy maintains followees) [medium]
    * generalize aggregates "reactions": extropy has only "count" at the current point [medium]
    * createdAt, updateAt [easy to medium]
    * support denormalization depending on other denormalized data [hard]
* proxy features and consistency level
    * provide a extropy.rc defining short cuts to run command against the proxy [easy]
    * proxy post-writes async: will return faster, but will be only "probably" consistent [medium]
    * warrant eventual consistency for proxied ops [medium]
    * proxy consistency level switchable request-per-request (inspiration form mongodb write concern) [harder]
    * reject write altering directly denormalized fields [medium]
    * reject write breaking foreign keys as an option [medium]
    * cascade delete as an option [medium]
* web admin interface
    * pin invariant foreman to specific worker [medium]
    * display state of asynchronous queues in the foremen [...]
    * cron-like programation of check and check-and-repair jobs [medium]

And these are medium to long term goals, stuff I've already have to do on specific cases in various past projects and
that I would like to make easier or trivial with extropy:
* hierarchical data: for instance, from a SOA based on immediate
    parent (and order), maintain children array and searchable lineage [medium to hard]
* multi-document eventual consistent transaction [medium]
* "command" pattern: one single insert to a "command" collection triggers various updates [hard]
* "changelog" pattern: store enough information on update to revert it, allowing to compute past state of database
    [hard]
* bucketted fan out at write
* "unbreakable" credit/withdraw and booking scenario (might be just a special case of command pattern) [hard]
* floating aggregates (for each user, maintain how many post / comments in the last day / week / ...) [hard]
