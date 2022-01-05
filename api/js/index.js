var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import wbindgen from "../pkg-wasm-bindgen/local_first.js";
import { Api } from "./bindings.mjs";
let API;
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
let pkg = Array.from(atob("AAIDAAAAAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAACAAAAOj///8AAAAAAAAAAAAAAAACAAAAdGl0bGUAAAUAAAAAAAAAAAgAAADo////AAAAAAAAAAAAAAAAAAIDAAAAAAAAAAAAAAAAAAAAAAAHAAAAdGl0bGUAAAXg////AAAAAAgAAADo////AAAAAAAAAAAAAAAAY29tcGxldGUCAAAACAAAAPT///8AAAAAAAAAAAgAAADo////AAAAAAAAAAAAAAAAY29tcGxldGUAAQAAAAAAAAAAAAAAAAAAAAAAAAcAAAAIAAAA4P///+D///8AAAAACAAAAOj///8AAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAIAAAB0aXRsZQAABQAAAAAAAAAABwAAAHRpdGxlAAAFpP7//wAAAAACAAAAdGFza3MAAAUAAAAAAAAAAAcAAAB0YXNrcwAABZD+//8AAAAABwAAAHRhc2tzAAAFpP7//wAAAAAHAAAAdGFza3MAAAW4/v//AAAAAAcAAAB0YXNrcwAABeD+//8AAAAABwAAAHRhc2tzAAAF/P7//wAAAAAHAAAAdGFza3MAAAUs////AAAAADj///8KAAAAdG9kb2FwcAcKAAAA/P3///gBAADs////AQAAAA=="), (c) => c.charCodeAt(0));
const init = () => __awaiter(void 0, void 0, void 0, function* () {
    if (API) {
        return yield API.createMemory(pkg);
    }
    else {
        const x = yield wbindgen();
        API = new Api();
        // @ts-ignore
        API.initWithInstance({ exports: x });
        return yield API.createMemory(pkg);
    }
});
class Wrapper {
    static create() {
        return __awaiter(this, void 0, void 0, function* () {
            const w = new Wrapper();
            w.sdk = yield init();
            return w;
        });
    }
    proxy(doc) {
        return new DocProxy(doc);
    }
}
const mkProxy = (doc) => {
    const cursor = doc.createCursor();
    const p = new Proxy({}, {
        //    apply?(target: T, thisArg: any, argArray: any[]): any,
        //    construct?(target: T, argArray: any[], newTarget: Function): object,
        //    defineProperty?(target: T, p: string | symbol, attributes: PropertyDescriptor): boolean,
        //    deleteProperty?(target: T, p: string | symbol): boolean,
        get(target, p, receiver) {
            if (Array.isArray(target)) {
                cursor.arrayIndex(Number(p));
            }
            else if (typeof target === "object") {
                const field = p.toString();
                try {
                    cursor.structField(field);
                }
                catch (e) {
                    cursor.mapKeyStr(field);
                }
            }
            else {
                throw new Error("Only arrays, fields (str), or structs supported.");
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
    });
};
class DocProxy {
    constructor(doc) {
        this.doc = doc;
    }
    mutate(fn) { }
}
const start = () => __awaiter(void 0, void 0, void 0, function* () {
    let localfirst = yield Wrapper.create();
    let w = window;
    w.localfirst = localfirst;
    console.log("Peer ID:", localfirst.sdk.getPeerId());
    //  w.doc = localfirst.proxy(localfirst.sdk.api.)
});
start();
export default Wrapper;
