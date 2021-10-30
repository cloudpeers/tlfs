import 'package:tlfs/tlfs.dart';
import 'package:test/test.dart';

void main() {
  test('tlfs test', () {
    final package = "../capi/assets/capi/include/todoapp.tlfs.rkyv";
    final sdk = Sdk.memory(package);
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
    doc.destroy();
    sdk.destroy();
    print('closed');
  });
}
