import 'dart:io';
import 'dart:typed_data';
import 'package:tlfs/tlfs.dart';
import 'package:test/test.dart';

void main() {
  test('tlfs test', () {
    final package = "../capi/assets/capi/include/todoapp.tlfs.rkyv";
    final f = File(package);
    f.openSync();
    final Uint8List bytes = f.readAsBytesSync();
    final sdk = Sdk.memory(bytes);
    final peer = sdk.peerId();
    print('peer: $peer');

    final doc = sdk.createDoc("todoapp");
    final id = doc.id();
    print('doc: $id');

    for (final doc_id in sdk.docs("todoapp")) {
      print(doc_id);
    }

    final cursor = doc.cursor();
    cursor.field("tasks");
    cursor.key(0);
    final cursor2 = cursor.clone();
    cursor.field("complete");
    final causal = cursor.enable();
    cursor2.field("title");
    final causal2 = cursor2.assignStr("something that needs to be done");
    causal.join(causal2);
    doc.apply(causal);
    assert(cursor.enabled());
    for (final str in cursor2.strs()) {
      print(str);
    }

    cursor.destroy();
    cursor2.destroy();

    doc.destroy();
    sdk.destroy();
    print('closed');
  });
}
