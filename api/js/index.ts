import wbindgen from "../pkg-wasm-bindgen/local_first.js"
import { Api, Doc, Sdk} from "./bindings.mjs"

let API: Api;

/**
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
 */
let pkg = Array.from(
	atob(
		"AAIDAAAAAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAACAAAAOj///8AAAAAAAAAAAAAAAACAAAAdGl0bGUAAAUAAAAAAAAAAAgAAADo////AAAAAAAAAAAAAAAAAAIDAAAAAAAAAAAAAAAAAAAAAAAHAAAAdGl0bGUAAAXg////AAAAAAgAAADo////AAAAAAAAAAAAAAAAY29tcGxldGUCAAAACAAAAPT///8AAAAAAAAAAAgAAADo////AAAAAAAAAAAAAAAAY29tcGxldGUAAQAAAAAAAAAAAAAAAAAAAAAAAAcAAAAIAAAA4P///+D///8AAAAACAAAAOj///8AAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAIAAAB0aXRsZQAABQAAAAAAAAAABwAAAHRpdGxlAAAFpP7//wAAAAACAAAAdGFza3MAAAUAAAAAAAAAAAcAAAB0YXNrcwAABZD+//8AAAAABwAAAHRhc2tzAAAFpP7//wAAAAAHAAAAdGFza3MAAAW4/v//AAAAAAcAAAB0YXNrcwAABeD+//8AAAAABwAAAHRhc2tzAAAF/P7//wAAAAAHAAAAdGFza3MAAAUs////AAAAADj///8KAAAAdG9kb2FwcAcKAAAA/P3///gBAADs////AQAAAA=="
	),
	(c) => c.charCodeAt(0)
)

const init = async () => {
	if (API) {
		return await API.createMemory(pkg);
	}
	else {
		const x = await wbindgen();

		API = new Api();
		// @ts-ignore
		API.initWithInstance({ exports: x });
		return await API.createMemory(pkg);
	}

};

class Wrapper {
	public sdk!: Sdk; 

	static async create() {
		const w = new Wrapper();
		w.sdk = await init();
		return w;
	}

  proxy(doc: Doc) {
    return new DocProxy(doc)
  }
}

const mkProxy = <T extends object>(doc: Doc) => {
  const cursor = doc.createCursor();
const p = new Proxy<T>({} as T,{

//    apply?(target: T, thisArg: any, argArray: any[]): any,
//    construct?(target: T, argArray: any[], newTarget: Function): object,
//    defineProperty?(target: T, p: string | symbol, attributes: PropertyDescriptor): boolean,
//    deleteProperty?(target: T, p: string | symbol): boolean,
    get(target: T, p: string | symbol, receiver: any) {
      if (Array.isArray(target)) {
        cursor.arrayIndex(Number(p));
      } else if (typeof target === "object") {
        const field = p.toString()
        try{
        cursor.structField(field)
        }catch (e) {
          cursor.mapKeyStr(field)
        }
      } else {
        throw new Error("Only arrays, fields (str), or structs supported.")
      }
      
      // return new object if not at a leaf
      // otherwise: return the actual value
    }
//    getOwnPropertyDescriptor?(target: T, p: string | symbol): PropertyDescriptor | undefined,
//    getPrototypeOf?(target: T): object | null,
//    has?(target: T, p: string | symbol): boolean,
//    isExtensible?(target: T): boolean,
//    ownKeys?(target: T): ArrayLike<string | symbol>,
//    preventExtensions?(target: T): boolean,
//    set?(target: T, p: string | symbol, value: any, receiver: any): boolean,
//    setPrototypeOf?(target: T, v: object | null): boolean,


})

}

class DocProxy {
  doc: Doc

  constructor(doc: Doc) {
    this.doc = doc
  }
  mutate<T>(fn: (_: T) => void) {}
}

const start = async () => {
  let localfirst = await Wrapper.create();
  let w = window as any;

  w.localfirst = localfirst;
  console.log("Peer ID:", localfirst.sdk.getPeerId())

//  w.doc = localfirst.proxy(localfirst.sdk.api.)
}
start();
export default Wrapper;