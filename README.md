couchbaselib
=================

Nodejs Couchbase Module


## include module  ##

```
npm install "git+https://github.com/Tlantic/couchbaselib.git#master"
```

```
var DB = require('couchbaselib');
````

### init (endpoint)

Example:

```
DB.initConnection(config.db.host);
);
```

### insert(bucketName, key, data, cb)

Example:

```
DB.insert(bucketName, key, data, function(err, result, retriveDocument) {
	  if (err) 
            cb(err);
	   
    	cb(null, result);
    }, true);
	
```

### update(bucketName, key, data, cb, retriveDocument)

Example:

```
DB.update(bucketName, key, data, function(err, result) {
	  if (err) 
            cb(err);

    	cb(null, result);
    }, true);
	
```
### remove(bucketName, key, cb)

Example:

```
DB.remove(bucketName, key, function(err, result) {
	  if (err) 
            cb(err);
	   
    	cb(null, result);
    }, true);
```

### get(bucketName, key, cb)

Example:

```
DB.get(bucketName, key, function(err, result) {
	  if (err) 
            cb(err);
	   
    	cb(null, result);
    });
```

### query(bucketName, design, view, cb, options)

Example:

```
DB.query('default', 'dev_user', 'by_type', function (err, result) {
		if (err){
			cb(err);
		}
		cb(null, result);
	});
```
```
viewQueryOptions:{
		stale: couchbase.ViewQuery.Update.BEFORE,
		skip : null,
		limit : null,
		order : null,
		reduce : null,
		group : null,
		group_level  : null,
		key : null,
		keys : null,
		range : null,   //{start:0, end:10, inclusive_end:true} Specifies a range of keys to retrieve from the index.
		id_range : null, //{start:0, end:10} Specifies a range of document id's to retrieve from the index.
		include_docs : null, //Flag to request a view request include the full document value.
		full_set : null //Flag to request a view request accross all nodes in the case of
	}
```


### getN1qlQuery(bucketName, query, cb)

Example:

```
DB.getN1qlQuery('default', "SELECT * FROM bee where type='user'", function (err, result) {
		if (err) {
			cb(err);
		}
		cb(null, result);
	});
```

