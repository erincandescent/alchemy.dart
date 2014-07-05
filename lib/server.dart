library alchemy.server;
import 'dart:async';
import 'dart:collection';
import 'package:bson/bson.dart';
import 'package:mongo_dart/mongo_dart.dart' as mdb;
import 'package:alchemy/core.dart';

class Database {
  String _connStr;
  mdb.Db _db;
  Future _openFuture;
  
  Database(String connectionString) 
      : _connStr = connectionString
      , _db = new mdb.Db(connectionString) {
  }
  
  Future<Connection> connect() {
    if(_openFuture == null) {
      _openFuture = _db.open();
    }
    
    return _openFuture.then((_) => new _Connection(_db));
  }
}

class _Connection extends Connection {
  mdb.Db _conn;
  DocumentType _dty;
  List<Document> dirtyObjects = [];
  
  Map<Type, Collection> _collections = new Map<Type, Collection>();
  
  _Connection(this._conn); 

  @override
  Collection operator [](Type ty) {   
    if(!_collections.containsKey(ty)) {
      ExDocumentType dty = ExDocumentType.map[ty];
      _collections[ty] = new _Collection(
          _conn.collection(dty.collectionName), dty);
    }
    return _collections[ty];
  }
  
  @override
  Future close() {
    return new Future.value(null);
  }
}

class _Collection extends Collection {
  mdb.DbCollection _coll;
  ExDocumentType     _dty;
  
  final Map<ObjectId, Future<Document>>
    _known = new Map<ObjectId, Future<Document>>();
  
  _Collection(this._coll, this._dty);
  
  @override
  Future<Document> get(Map fields) {
    return findOne(fields);
  }

  @override
  Future put(Document doc) {   
    return _coll.update({"_id":  doc["_id"]}, doc, upsert: true)
        .then((_) => doc);
  }

  @override
  Future<Document> refresh(Document doc) {
    return _coll.findOne({'_id': doc["_id"]}).then((map) {
      doc.clear();
      doc.addAll(map);
      return doc;
    });
  }

  @override
  void introduce(Document doc) {
    if(doc["_id"] == null) {
      doc["_id"] = new ObjectId();
    }
    _known[doc["_id"]] = new Future.value(doc);
  }
  
  Map _reformatQuery(Map query) {
    var nq = new LinkedHashMap();
    for(var k in query.keys) {
      var val = query[k];
      if(val is Document) {
        var edt = ExDocumentType.map[val.runtimeType];
        for(String l in edt.referenceFields) {
          nq["${k}.${l}"] = val[l];
        }
      } else {
        nq[k] = val;
      }
    }
    return nq;
  }
  
  Stream<Document> find(Map query, {int skip: 0, int limit: 0, Map sort}) {
    query = _reformatQuery(query);
    
    if(sort != null) {
      query = {"\$query" : query, "\$orderby" : _reformatQuery(sort)};
    }
    
    var cursor = _coll.find(query);

    cursor.skip = skip;
    cursor.limit = limit;
    
    var sc = new StreamController();
    
    return cursor.stream.asyncMap((m) {
      if(!_known.containsKey(m["_id"])) {
        var obj = _dty.mirror.newInstance(#revive, [m]).reflectee;
        var f = new Future.value(obj);
        _known[m["_id"]] = f;
        return f;
      } else {
        // Explicitly *DONT* override members here to avoid clobbering unsaved
        // changes
        return _known[m["_id"]];
      }
    });
  }
  
  Future<Document> findOne(Map query) {
    query = _reformatQuery(query);
    
    return _coll.findOne(query).then((m) {
      if(m == null) return null;
      
      if(!_known.containsKey(m["_id"])) {
        var f = new Future.value(_dty.mirror.newInstance(#revive, [m]).reflectee);
        _known[m["_id"]] = f;
        return f;
      } else {
        // Explicitly *DONT* override members here to avoid clobbering unsaved
        // changes
        return _known[m["_id"]];
      }
    });
  }
}
