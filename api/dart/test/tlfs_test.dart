import 'dart:io';
import 'dart:typed_data';
import 'package:tlfs/tlfs.dart';
import 'package:test/test.dart';

void main() {
  test('tlfs test', () async {
    await Process.run("tlfsc", ["-i", "test/todoapp.tlfs", "-o", "test/todoapp.tlfs.rkyv"]);
    final package = "test/todoapp.tlfs.rkyv";
    final f = File(package);
    f.openSync();
    final Uint8List bytes = f.readAsBytesSync();
    final api = Api.load();
    final sdk = await Sdk.createMemory(api, bytes);
    final peer = sdk.getPeerid();
    print('peer: $peer');

    final doc = sdk.createDoc("todoapp");
    final id = doc.id();
    print('doc: $id');

    for (final doc_id in sdk.docs("todoapp")) {
      print(doc_id);
    }

    final cursor = doc.createCursor();
    cursor.structField("tasks");
    cursor.arrayIndex(0);
    final cursor2 = cursor.clone();
    cursor.structField("complete");
    final causal = cursor.flagEnable();
    cursor2.structField("title");
    final causal2 = cursor2.regAssignStr("something that needs to be done");
    causal.join(causal2);
    doc.applyCausal(causal);
    assert(cursor.flagEnabled());
    for (final str in cursor2.regStrs()) {
      print(str);
    }

    cursor.drop();
    cursor2.drop();

    doc.drop();
    sdk.drop();
    print('closed');
  });
}
