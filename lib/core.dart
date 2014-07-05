library alchemy.core;
import 'dart:async';
import 'dart:mirrors';
import 'package:collection/collection.dart';
import 'package:uri/uri.dart';
import 'package:alchemy/src/fields.dart';
import 'package:stack_trace/stack_trace.dart' show Chain;

/** Every application is slightly different. Therefore, the method by which the
 *  database connection is obtained has been abstracted.
 *  
 *  You should return the appropriate database connection from this method.
 */
typedef Connection ConnectionGetter();

/** A connection to the database */
abstract class Connection {
  static ConnectionGetter _currentGetter;
  /** Get the current connection */
  static Connection get current => _currentGetter();
  /** Set the getter used to get the current connection */
  static void setCurrentGetter(ConnectionGetter g) {
    _currentGetter = g;
  }

  /// Return the named collection
  Collection operator [](Type ty);


  /// List of dirty objects
  List<Document> dirtyObjects;

  /// Save all dirty objects
  Future saveAll() {
    if(dirtyObjects.isNotEmpty) {
      var objs = dirtyObjects;
      dirtyObjects = [];
      return Future.forEach(objs, (ob) => ob.save()).then((_) => saveAll);
    } else return new Future.sync(() {});
  }

  void close();
}

/** A collection in the database. Collections contain [Document]s */
abstract class Collection {
  /** Get the document by its' reference fields */
  Future<Document> get(dynamic id);
  /** Refresh [doc] */
  Future<Document> refresh(Document doc);
  /** Put [doc] in the database */
  Future put(Document doc);
  /** Make the collection aware of the document */
  void introduce(Document doc);
}

Collection _getCollectionForMirror(ClassMirror mir) {
  assert(mir.isSubtypeOf(rDocument));
  return Connection.current[mir.reflectedType];
}

/// Document type annotation
class DocumentType {
  /** Annotates a class as a document type. If this is not provided, then
   *  the object will be treated as a primitive.
   *  
   *  [collectionName] is the name which will be used for the corresponding 
   *  database connection. If not provided, the name of the associated class
   *  converted to lower case will be used.
   *  
   *  [referenceFields] lists which fields of the object should be copied into
   *  a reference map. For projects using the client component, this should be
   *  at least enough to reproduce the document URI.
   *  
   *  [urlPattern] and [patternFields] are used in order to determine the URI
   *  to which requests should be sent to retrieve and update objects.
   *  
   *  [urlPattern] defines a pattern (as per [UrlPattern]) that the can be 
   *  matched and generated. Each entry in [patternFields] comprises a list of
   *  members of the document which are to be retrieved and passed to the above
   *  pattern.
   * 
   */
  const
      DocumentType({this.collectionName, this.referenceFields, this.urlPattern, this.patternFields});

  /// Name of collection containing objects
  final String collectionName;

  /// Fields to include in object references
  final List<String> referenceFields;

  /// The URL pattern defining how to perform requests
  final String urlPattern;
  /// The fields to be substituted into the URL pattern, in order
  final List<String> patternFields;
}

class ExDocumentType extends DocumentType {
  ExDocumentType(ClassMirror mir, DocumentType base)
      : super(collectionName: base.collectionName != null ? base.collectionName
          : MirrorSystem.getName(mir.simpleName), referenceFields: base.referenceFields !=
          null ? base.referenceFields : ["_id"], urlPattern: base.urlPattern,
          patternFields: base.patternFields != null ? base.patternFields :
          (base.referenceFields != null ? base.referenceFields : ["_id"])),
        mirror = mir {
    if (this.urlPattern != null) {
      print("Processing document type ${mir.simpleName}");
      template = new UriTemplate(this.urlPattern);
      parser = new UriParser(template);
    }
    processFields(this, mir);
  }

  containsAllReferences(Map<String, dynamic> map) {
    return referenceFields.map((f) => map.containsKey(f)).reduce((a, b) => a &&
        b);
  }

  final ClassMirror mirror;
  UriTemplate template;
  UriParser parser;
  Map<Symbol, dynamic> handlers   = {};
  Map<String, Importer> importers = {};
  Map<String, Exporter> exporters = {};

  static Map<Type, ExDocumentType> map = new Map<Type, ExDocumentType>();
}

void processDocumentAnnotations() {
  ClassMirror docType = reflectClass(DocumentType);
  for (LibraryMirror lib in currentMirrorSystem().libraries.values) {
    for (ClassMirror cls in lib.declarations.values.where((v) => v is
        ClassMirror)) {
      var meta = cls.metadata.where((m) => m.type == docType);
      if (meta.isNotEmpty) {
        DocumentType docType = meta.single.reflectee;
        ExDocumentType edt = new ExDocumentType(cls, docType);

        ExDocumentType.map[cls.reflectedType] = edt;
      }
    }
  }
}

abstract class _DocMixin {
  get _docType;
  
  void _raise(Invocation inv) {
    throw new NoSuchMethodError(this, inv.memberName, inv.positionalArguments,
        inv.namedArguments);
  }

  // XXX need to handle this for SubDocuments too
  noSuchMethod(Invocation invocation) {
    dynamic meth =
        _docType.handlers[invocation.memberName];
    if (meth == null) {
      _raise(invocation);
    }

    if (invocation.isGetter) {
      return meth(this);
    } else if (invocation.isSetter) {
      meth(this, invocation.positionalArguments[0]);
    } else {
      _raise(invocation);
    }
  }
}

/** A database backed document
 * 
 * Documents are all uniquely identified by a field named [id]. They can be 
 * fetched from the database by [Collection.get], and then updated via [refresh]
 * or saved via [save].
 * 
 * Create fields by declaring appropriately typed getters and setters:
 *      String get name; set name(String _);
 * 
 * For references to other documents,
 *      Future<OtherDoc> get name;
 *      set name(OtherDoc _);
 * 
 * (The getter returning a future but setter not may be somewhat unconventional,
 *  but it is also a necessary conceit)
 * 
 */
@proxy
abstract class Document extends DelegatingMap<String, dynamic> with _DocMixin {
  /// Is this object dirty?
  bool _dirty = false;

  /// Get the document type
  ExDocumentType get _docType => ExDocumentType.map[runtimeType];
  
  /// Make the object dirty
  _makeDirty() {
    if (!_dirty) {
      _dirty = true;
      Connection.current.dirtyObjects.add(this);
    }
  }

  /// Is the object dirty? (i.e. does this object contain unsaved changes?)
  get isDirty => _dirty;

  /// Create a brand new document
  ///
  /// If the passed [map] contains all defined reference fields, they will be
  /// used as is. Otherwise, [generateId] will be called in order to give the
  /// object a name
  Document([Map<String, dynamic> map])
      : super({}) {
    _createNew(map);
  }

  /// Invoked by [Collection] to revive an existing document. Your override
  /// should simply pass this through to the base class.
  Document.revive(Map<String, dynamic> baseMap)
      : super(baseMap);

  /// Invoked when importing into a new object.
  ///
  /// The default implementation filters out all non-reference fields, passes
  /// the result to the default Document constructor. If you wish for different
  /// behavior, then you should probably delegate your [new forImport] override
  /// to the default [Document] constructor [new]
  Document.forImport(Map<String, dynamic> beingImported)
      : super({}) {
    _createNew(new Map.fromIterable(
        _docType.referenceFields, value: (k) => beingImported[k])
        );
  }

  /// "Exports" this object
  /// The default behaviour of this object is to clone the map, filter out any
  /// private properties (i.e. those prefixed with an udnerscore) and then recursively
  /// export up to a maximum depth of [depth]
  Future<Map<String, dynamic>> export({depth: 2}) {
    var map = new Map.fromIterable(
        keys.where((k) => !k.startsWith("_")), 
        value: (k) => this[k]);
    
    if(depth > 0) {
      var exporters = _docType.exporters;
      return Future.forEach(exporters.keys.where((k) => containsKey(k)), (name) {
        var exp = exporters[name];
        return exp(this, depth).then((res) {
          map[name] = res;
        });
      }).then((_) {
        return map;
      });
    } else {
      return new Future.value(map);
    }
  }
  
  // Implements prescribed behavor for [new]
  void _createNew(Map<String, dynamic> map) {
    if(map != null) this.addAll(map);

    var docType = ExDocumentType.map[runtimeType];
    if (!docType.containsAllReferences(this)) {
      generateId();
    }

    assert(docType.containsAllReferences(this));

    _makeDirty();
    Connection.current[runtimeType].introduce(this);
  }

  /// Handles the import of data from external sources. The default
  /// implementation overrides fields unconditionally, excepting those defined
  /// as reference fields. You may wish to customise this behaviour however.
  ///
  /// Regardless, you should eventually pass your pruned map through to the base
  /// implementation
  ///
  /// The returned future will return this object.
  Future<Document> import(Map<String, dynamic> external) {
    var docType = ExDocumentType.map[runtimeType];
    var refFields = docType.referenceFields;

    // Validate ref fields are unchanged
    for (var f in refFields) {
      if (external[f] != this[f]) throw new ArgumentError(
          "Cannot override reference field ${f}");
    }

    // Import each field
    return Chain.track(Future.forEach(external.keys, (key) {
      Importer im = docType.importers[key];
      if (im != null) {
        return im(this, external[key]);
      } else {
        this[key] = external[key];
      }
    })).then((_) => this);
  }

  /// Generate a new ID for this object
  ///
  /// Should set all reference fields defined for this object to values which
  /// can reasonably be expected to be unique.
  void generateId();

  /// Saves the object
  Future save() {
    var conn = Connection.current;
    return conn[runtimeType].put(this).then((_) {
      this._dirty = false;
      conn.dirtyObjects.remove(this);
      return _;
    });
  }

  /// Refreshes the object from the database
  Future<Document> refresh() {
    return Connection.current[runtimeType].refresh(this);
  }

  String toString() => "Document(${runtimeType}, ${path()})";

  String path() {
    var edt = ExDocumentType.map[runtimeType];
    var vars = {};
    if(edt.template != null) {
      for (var v in edt.patternFields) {
        vars[v] = this[v];
      }

      return edt.template.expand(vars);
    } else {
      return edt.referenceFields.map((e) => this[e].toString()).join(", ");
    }
  }

  // Dirtiness detection
  @override
  void operator []=(String name, dynamic value) {
    _makeDirty();
    super[name] = value;
  }

  @override
  dynamic putIfAbsent(String name, dynamic ifAbsent()) {
    if (!containsKey(name)) {
      _makeDirty();
      return super.putIfAbsent(name, ifAbsent);
    } else return super[name];
  }

  @override
  void addAll(Map<String, dynamic> other) {
    super.addAll(other);
    _makeDirty();
  }

  @override
  void remove(String key) {
    super.remove(key);
    _makeDirty();
  }
}

Future<Document> importDocument(Type type, Map<String, dynamic> value) {
  var docType = ExDocumentType.map[type];
  var refFields = docType.referenceFields;
  if (refFields.map((k) => value.containsKey(k)).reduce((l, r) => l && r)) {
    // Contains all reference fields. Perfect!
    Map<String, dynamic> ref = new Map.fromIterable(refFields, value: (k) =>
        value[k]);

    return Connection.current[type].get(ref).then((obj) {
      if (obj == null) {
        // Doesn't exist, build new
        obj = docType.mirror.newInstance(#forImport, [value]).reflectee;
      }

      // Do an import
      return obj.import(value);
    });
  } else {
    // Doesn't contain all the reference fields. Must be new
    var obj = docType.mirror.newInstance(#forImport, [value]).reflectee;

    return obj.import(value);
  }
}

/** A Sub Document
 *
 * Sub Documents are stored inside their parent document and do not exist 
 * independently. They are implemented as views over the underlying maps.
 * 
 * Define them with the same abstract methods as you would do for [Documents]. 
 */
class SubDocument extends DelegatingMap<String, dynamic> with _DocMixin {
  SubDocument(): super(new Map<String, dynamic>());
  SubDocument.from(Map<String, dynamic> map): super(map);
}
