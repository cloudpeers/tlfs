import { default as wasmbin } from "../pkg-wasm-bindgen/local_first_bg.wasm"
import wbindgen from "../pkg-wasm-bindgen/local_first.js"
import { Api, Causal, Cursor, Doc, Sdk } from "./bindings"

let API: Api;

const init = async (pkg: number[]) => {
  if (API) {
    return await API.createMemory(pkg);
  }
  else {
    //const x = await wbindgen("../pkg-wasm-bindgen/local_first_bg.wasm");
    const x = await wbindgen(wasmbin)

    API = new Api();
    // @ts-ignore
    API.initWithInstance({ exports: x });
    return await API.createMemory(pkg);
  }

};

class LocalFirst {
  public sdk!: Sdk;

  static async create(pkg: number[]) {
    const w = new LocalFirst();
    w.sdk = await init(pkg);
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

const setValue = (cursor: Cursor, value: any): Causal => {
  let causal: Causal | undefined;
  // TODO: fix brute force
  if (Array.isArray(value)) {
    // overwrite complete array
    for (let index = 0; index < cursor.arrayLength(); index++) {
      const here = cursor.clone()
      here.arrayIndex(index)
      const c = here.arrayRemove()
      if (causal) {
        causal.join(c)
      } else {
        causal = c
      }
    }
    value.forEach((v, idx) => {
      const here = cursor.clone()
      here.arrayIndex(idx)
      const c = setPrimitiveValue(here, v)
      if (causal) {
        causal.join(c)
      } else {
        causal = c
      }
    })

  } else if (typeof value == 'object') {
    // delete complete object, if table
    if (cursor.pointsAtTable()) {
      for (const k in cursor.keys()) {
        const here = cursor.clone()
        here.mapKeyStr(k)
        const c = here.mapRemove()
        if (causal) {
          causal.join(c)
        } else {
          causal = c
        }
      }
    }

    // add
    Object.entries(value).forEach(([k, v]) => {
      const here = cursor.clone()
      if (here.pointsAtTable()) {
        here.mapKeyStr(k)
      } else {
        console.log("structField", k)
        here.structField(k)
      }
      const c = setPrimitiveValue(here, v)
      if (causal) {
        causal.join(c)
      } else {
        causal = c
      }
    })


  } else {
    // leaf value
    causal = setPrimitiveValue(cursor, value)
  }
  return causal!
}

const setPrimitiveValue = (cursor: Cursor, value: any): Causal => {

  switch (cursor.valueType()) {
    case null:
    case "null":
      throw new Error(`Not pointing at value type: ${cursor.valueType()}`)
    case "bool":
      if (Boolean(value)) {
        return cursor.flagEnable()
      } else {
        return cursor.flagDisable()
      }
    case "Reg<bool>":
      return cursor.regAssignBool(Boolean(value))
    case "Reg<u64>":
      return cursor.regAssignU64(BigInt(value))
    case "Reg<i64>":
      return cursor.regAssignI64(BigInt(value))
    case "Reg<string>":
      return cursor.regAssignStr(value.toString())
    default: {
      throw new Error("unreachable")
    }
  }
}

const mkProxy = <T extends object>(doc: Doc, cursor_?: Cursor): T => {

  return new Proxy<T>({} as T, {

    //    apply?(target: T, thisArg: any, argArray: any[]): any,
    //    construct?(target: T, argArray: any[], newTarget: Function): object,
    //    defineProperty?(target: T, p: string | symbol, attributes: PropertyDescriptor): boolean,
    //    deleteProperty?(target: T, p: string | symbol): boolean,
    get(target: T, p: string | symbol, receiver: any) {
      switch (p) {
        case Symbol.toPrimitive:
        case "valueOf":
          return undefined
        case "toString": {
          return function (...args: any[]) {
            console.log("toString", args)
            return "LOL"
          }
        }
      }

      const cursor = cursor_?.clone() || doc.createCursor()
      console.log("get", target, p, receiver)

      if (cursor.pointsAtArray()) {
        switch (p) {
          case 'filter': {
            return function (...args: any[]) {
              const arr = new Array(cursor.arrayLength()).map((_v, idx) =>
                get(doc, cursor.clone())({}, idx.toString(), undefined)
              )

              return arr.filter.apply(arr, args as any)
            }

          }
          case 'length': {
            return cursor.arrayLength()
          }
          case 'push': {
            const c2 = cursor.clone()
            return function (...args: any[]) {
              console.log("pushing with", c2, args)
              if (args.length > 0) {
                let causal: Causal | undefined;
                let arrayLen = c2.arrayLength()
                console.log("arrayLen", arrayLen)
                args.forEach((v, idx) => {

                  const c = c2.clone()
                  c.arrayIndex(idx + arrayLen)
                  const causal2 = setValue(c, v)
                  if (causal) {
                    causal.join(causal2)
                  } else {
                    causal = causal2
                  }
                })

                doc.applyCausal(causal!)
              }
              return true
            }

          }
          case 'map': {
            return function (...args: any[]) {
              console.log("map", cursor.arrayLength())
              const arr = new Array(cursor.arrayLength()).fill(undefined).map((_v, idx) => {
                const x = get(doc, cursor.clone())({}, idx.toString(), undefined)
                console.log("arr get", idx, x)
                return x
              }
              )

              return arr.map.apply(arr, args as any)
            }
          }
        }
      }
      traverse(cursor, p)

      if (cursor.pointsAtValue()) {
        switch (cursor.valueType()) {
          case "null": return undefined
          case "bool": return cursor.flagEnabled()
          case "Reg<bool>":
            return Array.from(cursor.regBools())[0]
          case "Reg<u64>":
            return Array.from(cursor.regU64s())[0]
          case "Reg<i64>":
            return Array.from(cursor.regI64s())[0]
          case "Reg<string>":
            return Array.from(cursor.regStrs())[0]
        }
      } else {
        // return new object if not at a leaf
        return mkProxy(doc, cursor.clone())
      }
    },
    getOwnPropertyDescriptor(target: T, p: string | symbol): PropertyDescriptor | undefined {
      // TODO: check `p`
      const cursor = cursor_?.clone() || doc.createCursor()
      const value = get(doc, cursor)(target, p, undefined)
      console.log("getOwnPropertDescriptor", target, p, value)
      return { configurable: true, enumerable: true, value }
    },
    //    getPrototypeOf?(target: T): object | null,
    //    has?(target: T, p: string | symbol): boolean,
    //    isExtensible?(target: T): boolean,
    ownKeys(target: T): ArrayLike<string | symbol> {

      console.log("ownKeys", target)
      const cursor = cursor_?.clone() || doc.createCursor()
      return Array.from(cursor.keys())
    },
    //    preventExtensions?(target: T): boolean,
    set(target: T, p: string | symbol, value: any, receiver: any): boolean {
      const cursor = cursor_?.clone() || doc.createCursor()
      console.log("set", target, p, value, receiver)

      traverse(cursor, p)

      const causal = setValue(cursor, value)

      doc.applyCausal(causal)
      return true
    }
    //    setPrototypeOf?(target: T, v: object | null): boolean,
  })

}

//const start = async () => {
//  let localfirst = await LocalFirst.create();
//  let w = window as any;
//
//  w.localfirst = localfirst;
//  console.log("Peer ID:", localfirst.sdk.getPeerId())
//
//
//  //  w.doc = localfirst.proxy(localfirst.sdk.api.)
//}
//start();
export default LocalFirst;
export * from './bindings'