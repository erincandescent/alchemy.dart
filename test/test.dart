import 'dart:async';
import 'package:alchemy/core.dart';
import 'package:alchemy/server.dart';
import 'package:uuid/uuid_server.dart';

var uuid = new Uuid();

@DocumentType(collectionName: "test_a")
class TestA extends Document {
  TestA()         : super();
  TestA.revive(m) : super.revive(m);
  
  String generateId() {
    this["id"] = uuid.v4();
  }
  
  String get a;
  void set a(String f);
  
  String get b;
  void set b(String f);
  
  Future<TestA> get friend;
  set friend(TestA f);
}

main() {
  processDocumentAnnotations();
  
  var a, b;
  Database db = new Database("mongodb://127.0.0.1:27017/testdb");
  db.connect().then((conn) {
    Connection.setCurrentGetter(() => conn);
    
    a = new TestA();
    a.a = "Hello";
    a.b = "World";
    
    assert(a.a == "Hello" && a.b == "World");
    
    return a.save();
  }).then((_) {
    b = new TestA();
    b.friend = a;
    a.friend = b;
    
    a.forEach((k, v) { print("A " + k + "=" + v.toString());});
    b.forEach((k, v) { print("B " + k + "=" + v.toString());});
    
    return a.save();
  }).then((_) {
    b.save();
  }).then((_) {
    return b.friend.then((v) { print("Identity? ${v == a}"); });
  }).then((_) {
    return a.friend.then((v) { print("Identity? ${v == b}"); });
  }).then((_) => Connection.current.close());
}