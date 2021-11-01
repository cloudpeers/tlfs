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

    final iter = sdk.docs();
    while (true) {
        final doc_id = iter.next();
        if (doc_id == null) {
            break;
        }
        print(doc_id);
    }
    iter.destroy();

    final cursor = doc.cursor();
    cursor.field("tasks");
    cursor.key(0);
    cursor.field("complete");
    final causal = cursor.enable();
    doc.apply(causal);
    assert(cursor.enabled());
    cursor.destroy();

    doc.destroy();
    sdk.destroy();
    print('closed');
  });
}
