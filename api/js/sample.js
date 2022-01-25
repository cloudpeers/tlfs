/**
 * from inside the `api` dir:
 *  run ./build-wasm.sh (with env OPTIMIZE=1 for release)
 *  then: npx --yes http-server js
 
schema:
 * todoapp {
    0.1.0 {
  .: Struct
  .title: MVReg<String>
  .tasks: Array
  .tasks.[]: Struct
  .tasks.[].title: MVReg<String>
  .tasks.[].complete: EWFlag
    }
}
cargo run --target x86_64-unknown-linux-gnu -- --input ../api/dart/test/todoapp.tlfs --output /dev/stdout | base64 -w0

Interact with the document via the js console:
```
window.doc.tasks[0].title = "abc"
...
 */
const lenses = Array.from(
	atob(
		'AAIDAAAAAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAACAAAAOj///8AAAAAAAAAAAAAAAACAAAAdGl0bGUAAAUAAAAAAAAAAAgAAADo////AAAAAAAAAAAAAAAAAAIDAAAAAAAAAAAAAAAAAAAAAAAHAAAAdGl0bGUAAAXg////AAAAAAgAAADo////AAAAAAAAAAAAAAAAY29tcGxldGUCAAAACAAAAPT///8AAAAAAAAAAAgAAADo////AAAAAAAAAAAAAAAAY29tcGxldGUAAQAAAAAAAAAAAAAAAAAAAAAAAAcAAAAIAAAA4P///+D///8AAAAACAAAAOj///8AAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAIAAAB0aXRsZQAABQAAAAAAAAAABwAAAHRpdGxlAAAFpP7//wAAAAACAAAAdGFza3MAAAUAAAAAAAAAAAcAAAB0YXNrcwAABZD+//8AAAAABwAAAHRhc2tzAAAFpP7//wAAAAAHAAAAdGFza3MAAAW4/v//AAAAAAcAAAB0YXNrcwAABeD+//8AAAAABwAAAHRhc2tzAAAF/P7//wAAAAAHAAAAdGFza3MAAAUs////AAAAADj///8KAAAAdG9kb2FwcAcKAAAA/P3///gBAADs////AQAAAA=='
	),
	(c) => c.charCodeAt(0)
)
import LocalFirst from "./lib/index.js"
const start = async () => {
	let localfirst = await LocalFirst.create("sample", lenses);
	let w = window;

	w.localfirst = localfirst;
	console.log("Peer ID:", localfirst.sdk.getPeerId())

	// let peers = Array.from(await localfirst.sdk.localPeers());
	// console.log("peers ", peers)


	w.doc = localfirst.proxy(await localfirst.sdk.createDoc("todoapp"))
	w.doc.title = "title"

	// let docs = Array.from(localfirst.sdk.docs("todoapp"));
	// let adoc = docs[0];
	// console.log("a doc=" + adoc)
	// w.doc = localfirst.sdk.openDoc(adoc)
	// console.log("doc ", w.doc)
	// console.log("doc.tasks[0].title ", w.doc.tasks[0].title)

	// console.log("1 - title now is " + w.doc.title)
	// w.doc.title = "foo"
	// console.log("2 - title now is " + w.doc.title)
	// w.doc.title = "bar"
	// console.log("3 - title now is " + w.doc.title)
	// w.doc.title = "theend"

}
start();