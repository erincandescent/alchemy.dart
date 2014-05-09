import 'dart:mirrors';
import 'dart:async';
import 'package:alchemy/core.dart';

/// A getter
typedef dynamic Getter(dynamic object);
/// A setter
typedef    void Setter(dynamic object, dynamic value);
/// An importer. Importers are used when special logic is required to decode an
/// object being brought in from externally. Should probably be paired with an
/// [Exporter]
typedef Future Importer(dynamic object, dynamic value);
/// An exporter. An exporter converts a [value] into a form suitable for external
/// serialization
typedef Future Exporter(dynamic object, dynamic value);

abstract class FieldType {
  /// Returns a getter
  Getter getter(Type type, String name);
  // Returns a setter
  Setter setter(Type type, String name);
  
  Importer importer(Type type, String name) => null;
  Exporter exporter(Type type, String name) => null;
}

class _BasicType extends FieldType { 
  @override
  Getter getter(Type type, String name) {
    return (object) => object[name];
  }

  @override
  Setter setter(Type type, String name) {
    return (object, value) { object[name] = value; };
  }
}

class _DocumentType extends FieldType { 
  @override
  Getter getter(Type type, String name) {
    return (object) {
      if(object.containsKey(name)) {
        return Connection.current[type].get(object[name]); 
      } else {
        return new Future.value(null);
      }
    };
  }

  @override
  Setter setter(Type type, String name) {
    return (object, value) {
      var map = {};
      for(String key in ExDocumentType.map[type].referenceFields) {
        map[key] = value[key];
      }
      object[name] = map;
    };
  }
  
  @override
  Importer importer(Type type, String name) {
    Setter set = setter(type, name);
    return (object, value) => importDocument(type, value).then((v) {
      set(object, v);
    });
  }
}

class _SubDocumentType extends FieldType { 
  @override
  Getter getter(Type type, String name) {
    return (object) {
      var sd = object[name];
      if(sd is! SubDocument) {
        sd = (rm as ClassMirror).newInstance(#from, [sd]);
        object[name] = sd;
      }
    };
  }

  @override
  Setter setter(Type type, String name) {
    return (object, value) { object[name] = value; };
  }
}

var _basicType = new _BasicType();
var _types = {
  reflectClass(Document)    : new _DocumentType(),
  reflectClass(SubDocument) : new _SubDocumentType()
};

TypeMirror rFuture      = reflectClass(Future);

void processFields(ExDocumentType edt, ClassMirror class_) {
  for(var decl in class_.declarations.values) {
    if(decl is MethodMirror && decl.isAbstract) {
      TypeMirror type;
      
      if(decl.isGetter) {
        type = decl.returnType;
        // If a future, probably actually a document getter...
        if(type is ClassMirror && type.originalDeclaration == rFuture) {
          type = type.typeArguments[0];
        }
      } else if(decl.isSetter){
        type = decl.parameters[0].type;
      } else continue;
     
      FieldType handler = _basicType;
      if(type is ClassMirror) for(ClassMirror baseType in _types.keys) {
        if(type.isSubclassOf(baseType)) {
          handler = _types[baseType];
          break;
        }
      }
      
      String name = MirrorSystem.getName(decl.simpleName);
      if(decl.isGetter) {
        edt.handlers[decl.simpleName] = handler.getter(type.reflectedType, name);
        Importer imp = handler.importer(type.reflectedType, name);
        if(imp != null) {
          print("Importer on ${name}");
          edt.importers[name] = imp;
        }
      } else {
        name = name.substring(0, name.length - 1);
        edt.handlers[decl.simpleName] = handler.setter(type.reflectedType, name);
      }
    }
  }
}