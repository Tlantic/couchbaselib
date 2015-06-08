couchbaselib
=================

Nodejs Couchbase Module


## include module  ##

```
npm install "git+https://github.com/Tlantic/couchbaselib.git#master"
```

```javascript
var DB = require('couchbaselib');
````

## start connection  ##

### initConnection(endpoint)

Example:

```javascript
DB.initConnection(config.db.host);
);
```

### getBucket(bucketName, callback) - Check if bucket exist

Example:

```javascript
DB.getBucket('mrs', function(err, msg){
    if(err)
    {
      callback(err);
    }
    else{
      callback(null, msg);
    }
  });
);
```

## Database Model  ##

### Defining a Model

```javascript
var couchbaselib = require('couchbaselib');
var Schema = couchbaselib.Schema;

var userSchema = new Schema('user',{properties: {
	firstname: {
		type: 'string',
		minLength: 2,
		maxLength: 15
	},
	lastname: {
		type: 'string',
		minLength: 2,
		maxLength: 25,
	},
	state: {
		type: 'string',
		minLength: 2,
		maxLength: 15,
		default: "A"
	}
},
	required: ['firstname']});

var methods = {
	getRefreshToken: function(key, cb) {
			couchbaselib.db.query('default', 'dev_auth', 'by_all', cb, {
					key: key
			});
	}
};


exports = module.exports = couchbaselib.model('User', userSchema, 'default', methods);
```

### Model create fields by default
- _uId: uuid.v4()
- _type: document type (ex: user)
- _createDate: unix date format
- _updateDate: unix date format

### Model default functions

- findById: function(bucket, key, cb);
- save: function(callback);
- update: function(id, data, callback);
- remove: function(id, callback)

###Example

```javascript
var user = new User({firstname:'John', lastname:"Doe"});
	user.save(function(err, data){
		if(err)
			res.send(err);
		User.print();
		User.findById('default', data._type ,data._uId ,function(err, data){
			httpResponse.success(data, res);
		});

	});
```

## Database API  ##

### insert(bucketName, key, data, cb)

Example:

```javascript
DB.insert(bucketName, key, data, function(err, result, retriveDocument) {
	  if (err)
            cb(err);

    	cb(null, result);
    }, true);

```

### update(bucketName, key, data, cb, retriveDocument)

Example:

```javascript
DB.update(bucketName, key, data, function(err, result) {
	  if (err)
            cb(err);

    	cb(null, result);
    }, true);

```
### remove(bucketName, key, cb)

Example:

```javascript
DB.remove(bucketName, key, function(err, result) {
	  if (err)
            cb(err);

    	cb(null, result);
    }, true);
```

### get(bucketName, key, cb)

Example:

```javascript
DB.get(bucketName, key, function(err, result) {
	  if (err)
            cb(err);

    	cb(null, result);
    });
```

### query(bucketName, design, view, cb, options)

Example:

```javascript
DB.query('default', 'dev_user', 'by_type', function (err, result) {
		if (err){
			cb(err);
		}
		cb(null, result);
	});
```
```javascript
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
