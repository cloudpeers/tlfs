import wbindgen from "../pkg-wasm-bindgen/local_first.js"
import { Api, Cursor, Doc, Sdk } from "./bindings.js"

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

  proxy<T extends object>(doc: Doc): T {
    return mkProxy<T>(doc)
  }
}
const traverse = (cursor: Cursor, p: any) => {
  if (cursor.pointsAtArray()) {
    cursor.arrayIndex(Number(p));
  } else if (cursor.pointsAtStruct()) {
    const field = p.toString()
    cursor.structField(field)
  } else if (cursor.pointsAtTable()) {
    const field = p.toString()
    cursor.mapKeyStr(field)
  } else {
    throw new Error("Only arrays, fields (str), or structs supported.")
  }
}

const get = <T>(doc: Doc, cursor_?: Cursor) => (target: T, p: string | symbol, receiver: any) => {
  const cursor = cursor_ || doc.createCursor()
  console.log("get", target, p, receiver)

  traverse(cursor, p)

  if (cursor.pointsAtValue()) {
    switch (cursor.valueType()) {
      case "null": { return undefined; }
      case "bool": { return cursor.flagEnabled() }
      case "Reg<bool>":
        { return Array.from(cursor.regBools())[0] }
      case "Reg<u64>":
        { return Array.from(cursor.regU64s())[0] }
      case "Reg<i64>":
        { return Array.from(cursor.regI64s())[0] }
      case "Reg<string>":
        { return Array.from(cursor.regStrs())[0] }
    }
  } else {

    // return new object if not at a leaf
    return mkProxy(doc, cursor.clone())
  }

}
const mkProxy = <T extends object>(doc: Doc, cursor_?: Cursor): T => {

  return new Proxy<T>({} as T, {

    //    apply?(target: T, thisArg: any, argArray: any[]): any,
    //    construct?(target: T, argArray: any[], newTarget: Function): object,
    //    defineProperty?(target: T, p: string | symbol, attributes: PropertyDescriptor): boolean,
    //    deleteProperty?(target: T, p: string | symbol): boolean,
    get(target: T, p: string | symbol, receiver: any) {
      const cursor = cursor_ || doc.createCursor()
      console.log("get", target, p, receiver)

      traverse(cursor, p)

      if (cursor.pointsAtValue()) {
        switch (cursor.valueType()) {
          case "null": { return undefined; }
          case "bool": { return cursor.flagEnabled() }
          case "Reg<bool>":
            { return Array.from(cursor.regBools())[0] }
          case "Reg<u64>":
            { return Array.from(cursor.regU64s())[0] }
          case "Reg<i64>":
            { return Array.from(cursor.regI64s())[0] }
          case "Reg<string>":
            { return Array.from(cursor.regStrs())[0] }
        }
      } else {

        // return new object if not at a leaf
        return mkProxy(doc, cursor.clone())
      }

    },
    getOwnPropertyDescriptor(target: T, p: string | symbol): PropertyDescriptor | undefined {
      // TODO: check `p`
      const value = get(doc, cursor_)(target, p, undefined)
      return { configurable: true, enumerable: true, value }
    },
    //    getPrototypeOf?(target: T): object | null,
    //    has?(target: T, p: string | symbol): boolean,
    //    isExtensible?(target: T): boolean,
    ownKeys(target: T): ArrayLike<string | symbol> {

      const cursor = cursor_ || doc.createCursor()
      return Array.from(cursor.keys())
    },
    //    preventExtensions?(target: T): boolean,
    set(target: T, p: string | symbol, value: any, receiver: any): boolean {
      const cursor = cursor_ || doc.createCursor()
      console.log("set", target, p, value, receiver)

      traverse(cursor, p)

      if (Array.isArray(value)) {
        // TODO
      } else if (typeof value == 'object') {

        // TODO
      } else {
        let causal;
        switch (cursor.valueType()) {
          case null:
          case "null": {
            throw new Error("Not pointing at value type")
          }
          case "bool": {
            if (Boolean(value)) {
              causal = cursor.flagEnable()
            } else {
              causal = cursor.flagDisable()
            }
            break;
          }
          case "Reg<bool>":
            {
              causal = cursor.regAssignBool(Boolean(value))
              break;
            }
          case "Reg<u64>":

            {
              causal = cursor.regAssignU64(BigInt(value))
              break;
            }

          case "Reg<i64>":
            {
              causal = cursor.regAssignI64(BigInt(value))

              break;
            }
          case "Reg<string>":
            {
              causal = cursor.regAssignStr(value.toString())
              break;
            }
          default: {
            throw new Error("unreachable")
          }
        }
        doc.applyCausal(causal)
      }
      return true
    }
    //    setPrototypeOf?(target: T, v: object | null): boolean,


  })

}

class DocProxy {
  doc: Doc

  constructor(doc: Doc) {
    this.doc = doc
  }
  mutate<T>(fn: (_: T) => void) { }
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