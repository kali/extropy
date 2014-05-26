# extropy

Language- and Framework-agnostic declarative denormalization for MongoDB.

## What's the point

In order to get the most of a MongoDB database, developpers have to jump through the painful hoops of denormalization.
extropy aims at getting this complexity away.

extropy main component is a MongoDB proxy. All interactions (at least the ones performing write ops) from the
applications must go through the proxy.
Once this setup is performed, extropy will handle all ancilliary writes for each write op. It also supports adding
rules on pre-existing data.

extropy is coded in scala, but strictly no scala knowledge is required for using it, as it builds upon
MongoDB protocol.

## Example

Imagine a minimalist blog engine. It has two collections: users, and posts.
Posts containts comments as embedded subdocuments.

A fully-normalized data set could look like that (note that I have omited many fields... like the actual text for the posts):

```javascript
> db.posts.find()
{ "_id" : "post1", "title" : "Title for Post 1", "authorId" : "liz" }
{ "_id" : "post2", "title" : "Title for Post 2", "authorId" : "liz", "comments" : [ { "_id" : "comment1", "authorId" : "jack" } ]  }

> db.users.find()
{ "_id" : "jack", "name" : "John Francis \"Jack\" Donaghy" }
{ "_id" : "liz", "name" : "Elizabeth Lemon" }
```

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

The purpose of extropy is to get a declarative way to manage these denormalized fields: let's define a first "rule", 
the one maintaining a "authorName" field in the "posts" documents.

```javascript
{ "rule" : { "from" : "blog.posts", "follow" : "authorId", "to" : "blog.users" },
    "authorName" : "name" }
```

Each rule definition must start with a "rule" field. Its value describe how documents are "tied", from the document
containing the denormalization, to the source-of-authority document.
In this instance, the tie is materialized by the "authorId" field of the "posts" collection, pointing to a document
in users (and more specifically its _id).

Then comes one or several expressions describing what fields are to be denormalized and how. A string value is
a plain copy, while objects denotes more complex operations.
Here, we will just copy "name" from the found "user" document in a field called authorName.

We want authorName for comments authors too:

```javascript
{ "rule" : { "from" : "blog.posts.comments", "follow" : "authorId", "to" : "blog.users" },
    "authorName" : "name" }
```

The only difference is the "from" container definition: it was a collection, now it's an array of subdocuments.

Next comes the "comment Count in posts" rule:

```javascript
{ "rule" : { "from" : "blog.posts", "unwind" : "comments" },
    "commentCount" : { "js" : "function(cursor) cursor.size()" } }
```

Here we are using a different "tie": "unwind" expand an array of subdocuments. The expression is a bit more complicated
than a field copy, we are interested in the number of elements in the cursor obtained by "unwinding".
Note that "unwind" will lead to a subdocument cursor whereas "follow" leads to a single document.

I'm using the nice "lambda-style" syntax extension that nashorn, the new Java 8 JS engine, borrows from mozilla 1.8
javascript version. Nothing tricky:

```javascript
function sqr(x) x*x
// is equivalent to
// function sqr(x) { return x*x }
```

Next come the post and comment counters in the users collection:

```javascript
{ "rule" : { "from" : "blog.users", "search" : "blog.posts", "by" : "authorId" },
    "postCount" : { "js" : "function(cursor) cursor.size()" } }
{ "rule" : { "from" : "blog.users", "search" : "blog.posts.comments", "by" : "authorId" },
    "commentCount" : { "js" : "function(cursor) cursor.size()" } }
```

Introducing the "search" tie, which is just a reversed "follow". It leads to a cursor of documents in the first rule,
a cursor of subdocuments in the second.


```javascript
{ "rule" : { "from" : "blog.posts", "same" : "blog.posts" },
    "searchableTitle" : { "js" : "function(doc) doc.title.toLowerCase()", "using" : [ "title" ] } }
```

Finaly, the "same" tie stays at the same place. As "follow", it leads to a single place.
In this case, instead of a cursor, extropy pass the found document to the JavaScript function.
The "using" parameter is necessary for extropy to know which fields from the document it needs to keep track of.

JavaScript reactions allow to make arbitrary complex computation at denormalization time. Let's imagine a scenario
where the comments come with a rating, and we need to maintain the average rating in the post document. (I have
re-formatted the document with non-json compatible carriage return, but you get the idea).

```javascript
{ "rule" : { "from" : "blog.posts", "same" : "blog.posts" },
    "averageRating" : { "js" : "function(cursor) {
                            var total=0;
                            for each (comment in cursor)
                                total += comment.rating;
                            return total / cursor.size();
                        }",
                    "using" : [ "title" ] } }
```

## Containers, Ties and Reactions

### Containers

Containers are "places" where denormalized field can occur:
* TopLevel: for documents. They are denoted by the database name and the collection name separated by a dot, like
    "blog.posts"
* Nested: for sub-documents in an TopLevel array.
    Just add a dot and the array field name to the collection: "blog.posts.comments".

Notes:
* These syntaxes break when the collection name contains a dot, which is unfortunately allowed but... I have a plan.
* Nesting a document directly, object-in-object, with no array is in the roadmap.
* Nested subdocument MUST contain an _id.
* Only one level of sub document is supported.

### Ties

Four "ties" are actually supported:
* follow: for N-to-1 situation,
* search: just the opposite,
* unwind: like the $unwind in the aggregation framework, dig down in an array of subdocuments,
* same: stay in the same document.

### Reactions

Two types of reaction are supported:
* copy a field ("authorName" : "name")
* JavaScript expression. If the tie for a rule leads to one single document (like "same", or "follow") then the
    document is passed to a user-defined JS function.
    For ties leading to 0 to N documents, like "search" or "unwind", a "cursor" is passed instead.
    Listing used fields in the "using" parameter is necessary for extropy to know what fields update it needs to watch.

## Run the example

You need java in your $PATH and it has to point to a Java 8 setup. It will *not* work with earlier versions.
You'll also need a MongoDB server you can safely play with, and I'll be using the mongo client to demonstrate the
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

Let's load our first rule. We'll pick the "copy author name as authorName in each post" one. 
We need to call a command on extropy. The syntax may look a bit convoluted, but it mimicks
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
    * alternative json syntax for collections with dot in the name [easy]
    * support more tie types: 1-to-1 embedded, 1-to-1 by reference [easy]
    * support N-to-N ties (developper maintains follower array, extropy maintains followees) [medium]
    * createdAt, updatedAt [easy to medium]
    * support denormalization depending on other denormalized data [hard]
* proxy features and consistency level
    * replica set support (mongoS-based cluster is ok, standalone too, but replica set needs some hacking) [medium]
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
