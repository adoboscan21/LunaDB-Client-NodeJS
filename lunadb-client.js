var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import tls from "node:tls";
import fs from "node:fs";
import { Buffer } from "node:buffer";
import { BSON } from "bson";
// --- Protocol Constants ---
const CMD_COLLECTION_CREATE = 1;
const CMD_COLLECTION_DELETE = 2;
const CMD_COLLECTION_LIST = 3;
const CMD_COLLECTION_INDEX_CREATE = 4;
const CMD_COLLECTION_INDEX_DELETE = 5;
const CMD_COLLECTION_INDEX_LIST = 6;
const CMD_COLLECTION_ITEM_SET = 7;
const CMD_COLLECTION_ITEM_SET_MANY = 8;
const CMD_COLLECTION_ITEM_GET = 9;
const CMD_COLLECTION_ITEM_DELETE = 10;
const CMD_COLLECTION_QUERY = 12;
const CMD_COLLECTION_ITEM_DELETE_MANY = 13;
const CMD_COLLECTION_ITEM_UPDATE = 14;
const CMD_COLLECTION_ITEM_UPDATE_MANY = 15;
const CMD_COLLECTION_UPDATE_WHERE = 16;
const CMD_COLLECTION_DELETE_WHERE = 17;
const CMD_AUTHENTICATE = 18;
const CMD_BEGIN = 25;
const CMD_COMMIT = 26;
const CMD_ROLLBACK = 27;
const STATUS_OK = 1;
const STATUS_NOT_FOUND = 2;
function getStatusString(status) {
    const statuses = {
        1: "OK",
        2: "NOT_FOUND",
        3: "ERROR",
        4: "BAD_COMMAND",
        5: "UNAUTHORIZED",
        6: "BAD_REQUEST",
    };
    return statuses[status] || "UNKNOWN_STATUS";
}
// 🚀 HIGH PERFORMANCE ZERO-COPY FRAME BUILDER
// Soporta Uint8Array (BSON) nativo y lo convierte a Node Buffer sin duplicar memoria
function buildFrame(cmd, ...args) {
    let size = 1; // 1 byte para el comando
    const parsedArgs = args.map((arg) => {
        if (typeof arg === "string") {
            const b = Buffer.from(arg, "utf8");
            size += 4 + b.length;
            return { type: "buf", data: b };
        }
        else if (arg instanceof Uint8Array) {
            // ZERO-COPY CAST: Usamos el buffer de memoria subyacente directamente
            const b = Buffer.isBuffer(arg)
                ? arg
                : Buffer.from(arg.buffer, arg.byteOffset, arg.byteLength);
            size += 4 + b.length;
            return { type: "buf", data: b };
        }
        else if (Array.isArray(arg)) {
            size += 4; // 4 bytes para la cantidad de llaves
            const bufs = arg.map((s) => Buffer.from(s, "utf8"));
            for (const b of bufs)
                size += 4 + b.length;
            return { type: "arr", data: bufs };
        }
        throw new Error("Invalid argument type for frame builder");
    });
    const buf = Buffer.allocUnsafe(size);
    let offset = 0;
    buf.writeUInt8(cmd, offset++);
    for (const arg of parsedArgs) {
        if (arg.type === "buf") {
            const b = arg.data;
            buf.writeUInt32LE(b.length, offset);
            offset += 4;
            b.copy(buf, offset);
            offset += b.length;
        }
        else if (arg.type === "arr") {
            const arr = arg.data;
            buf.writeUInt32LE(arr.length, offset);
            offset += 4;
            for (const b of arr) {
                buf.writeUInt32LE(b.length, offset);
                offset += 4;
                b.copy(buf, offset);
                offset += b.length;
            }
        }
    }
    return buf;
}
// --- Internal Connection Wrapper ---
class LunaDBConnection {
    constructor() {
        this.socket = null;
        this.isClosed = false;
        this.responseBuffer = Buffer.allocUnsafe(0);
        this.responseWaiter = null;
        this.errorWaiter = null;
    }
    connect(host, port, user, pass, cert, rejectAuth = true) {
        return new Promise((resolve, reject) => {
            const options = {
                host,
                port,
                rejectUnauthorized: rejectAuth,
            };
            if (cert)
                options.ca = [fs.readFileSync(cert)];
            this.socket = tls.connect(options, () => __awaiter(this, void 0, void 0, function* () {
                this.socket.on("data", (chunk) => {
                    if (this.responseBuffer.length === 0) {
                        this.responseBuffer = chunk;
                    }
                    else {
                        this.responseBuffer = Buffer.concat([this.responseBuffer, chunk]);
                    }
                    this.tryProcessResponse();
                });
                this.socket.on("close", () => {
                    this.isClosed = true;
                    this.triggerError(new Error("Socket closed"));
                });
                this.socket.on("error", (err) => {
                    this.isClosed = true;
                    this.triggerError(err);
                });
                if (user && pass) {
                    try {
                        const frame = buildFrame(CMD_AUTHENTICATE, user, pass);
                        const res = yield this.sendFrame(frame);
                        if (res.status !== STATUS_OK)
                            return reject(new Error(`Auth failed: ${res.message}`));
                    }
                    catch (e) {
                        return reject(e);
                    }
                }
                resolve();
            }));
            this.socket.on("error", reject);
        });
    }
    triggerError(err) {
        if (this.errorWaiter) {
            this.errorWaiter(err);
            this.responseWaiter = null;
            this.errorWaiter = null;
        }
    }
    tryProcessResponse() {
        if (!this.responseWaiter)
            return;
        while (this.responseBuffer.length >= 5) {
            const msgLen = this.responseBuffer.readUInt32LE(1);
            const reqForDataLen = 5 + msgLen + 4;
            if (this.responseBuffer.length < reqForDataLen)
                return;
            const dataLen = this.responseBuffer.readUInt32LE(5 + msgLen);
            const totalLen = reqForDataLen + dataLen;
            if (this.responseBuffer.length < totalLen)
                return;
            const status = this.responseBuffer.readUInt8(0);
            const message = this.responseBuffer.toString("utf8", 5, 5 + msgLen);
            const data = Buffer.from(this.responseBuffer.subarray(reqForDataLen, totalLen));
            if (this.responseBuffer.length === totalLen) {
                this.responseBuffer = Buffer.allocUnsafe(0);
            }
            else {
                this.responseBuffer = this.responseBuffer.subarray(totalLen);
            }
            const waiter = this.responseWaiter;
            this.responseWaiter = null;
            this.errorWaiter = null;
            waiter({ status, message, data });
            if (!this.responseWaiter)
                return;
        }
    }
    sendFrame(frame) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.isClosed)
                throw new Error("Connection is dead");
            return new Promise((resolve, reject) => {
                this.responseWaiter = resolve;
                this.errorWaiter = reject;
                this.socket.write(frame, (err) => {
                    if (err) {
                        this.isClosed = true;
                        this.triggerError(err);
                    }
                });
            });
        });
    }
    close() {
        var _a;
        this.isClosed = true;
        (_a = this.socket) === null || _a === void 0 ? void 0 : _a.destroy();
    }
}
// --- Stateful Transaction Class ---
export class Tx {
    constructor(client, conn) {
        this.client = client;
        this.conn = conn;
        this.closed = false;
    }
    execute(frame) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closed)
                throw new Error("Transaction is already closed");
            try {
                const res = yield this.conn.sendFrame(frame);
                if (res.status !== STATUS_OK)
                    throw new Error(res.message);
                return res;
            }
            catch (err) {
                this.closed = true;
                this.client.release(this.conn);
                throw err;
            }
        });
    }
    collectionItemSet(col_1, value_1) {
        return __awaiter(this, arguments, void 0, function* (col, value, key = "") {
            const frame = buildFrame(CMD_COLLECTION_ITEM_SET, col, key, BSON.serialize(value));
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionItemSetMany(col, items) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_ITEM_SET_MANY, col, BSON.serialize({ array: items }));
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionItemUpdate(col, key, patch) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_ITEM_UPDATE, col, key, BSON.serialize(patch));
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionItemUpdateMany(col, items) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_ITEM_UPDATE_MANY, col, BSON.serialize({ array: items }));
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionItemDelete(col, key) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_ITEM_DELETE, col, key);
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionItemDeleteMany(col, keys) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_ITEM_DELETE_MANY, col, keys);
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionUpdateWhere(col, query, patch) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_UPDATE_WHERE, col, BSON.serialize(query), BSON.serialize(patch));
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    collectionDeleteWhere(col, query) {
        return __awaiter(this, void 0, void 0, function* () {
            const frame = buildFrame(CMD_COLLECTION_DELETE_WHERE, col, BSON.serialize(query));
            const res = yield this.execute(frame);
            return { Message: res.message };
        });
    }
    rollback() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closed)
                return { Message: "Already closed" };
            const frame = Buffer.from([CMD_ROLLBACK]);
            const res = yield this.conn.sendFrame(frame);
            this.closed = true;
            this.client.release(this.conn);
            return { Message: res.message };
        });
    }
    commit() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closed)
                throw new Error("Transaction is already closed");
            const frame = Buffer.from([CMD_COMMIT]);
            const res = yield this.conn.sendFrame(frame);
            this.closed = true;
            this.client.release(this.conn);
            if (res.status !== STATUS_OK)
                throw new Error(res.message);
            return { Message: res.message };
        });
    }
}
// --- Main Client (Connection Pool Manager) ---
export class LunaDBClient {
    constructor(host, port, username, password, serverCertPath, rejectUnauthorized = true, poolSize = 100) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.serverCertPath = serverCertPath;
        this.rejectUnauthorized = rejectUnauthorized;
        this.poolSize = poolSize;
        this.pool = [];
        this.waiters = [];
        this.activeConnections = 0;
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            const testConn = yield this.acquire();
            this.release(testConn);
        });
    }
    close() {
        for (const conn of this.pool)
            conn.close();
        this.pool = [];
        this.activeConnections = 0;
        for (const waiter of this.waiters)
            waiter.reject(new Error("Client closed"));
        this.waiters = [];
    }
    acquire() {
        return __awaiter(this, void 0, void 0, function* () {
            this.pool = this.pool.filter((c) => !c.isClosed);
            if (this.pool.length > 0)
                return this.pool.pop();
            if (this.activeConnections < this.poolSize) {
                this.activeConnections++;
                const conn = new LunaDBConnection();
                try {
                    yield conn.connect(this.host, this.port, this.username, this.password, this.serverCertPath, this.rejectUnauthorized);
                    conn.socket.once("close", () => {
                        this.activeConnections--;
                        this.pool = this.pool.filter((c) => c !== conn);
                    });
                    return conn;
                }
                catch (err) {
                    this.activeConnections--;
                    throw new Error(`LunaDB connection failed: ${err.message}`);
                }
            }
            return new Promise((resolve, reject) => {
                this.waiters.push({ resolve, reject });
            });
        });
    }
    release(conn) {
        if (conn.isClosed) {
            if (this.waiters.length > 0) {
                const waiter = this.waiters.shift();
                this.acquire().then(waiter.resolve).catch(waiter.reject);
            }
            return;
        }
        if (this.waiters.length > 0) {
            const waiter = this.waiters.shift();
            waiter.resolve(conn);
        }
        else {
            this.pool.push(conn);
        }
    }
    execute(frame, parser) {
        return __awaiter(this, void 0, void 0, function* () {
            const conn = yield this.acquire();
            try {
                const res = yield conn.sendFrame(frame);
                if (res.status !== STATUS_OK && res.status !== STATUS_NOT_FOUND) {
                    throw new Error(`${getStatusString(res.status)}: ${res.message}`);
                }
                this.release(conn);
                return parser(res);
            }
            catch (err) {
                conn.close();
                this.release(conn);
                throw err;
            }
        });
    }
    begin() {
        return __awaiter(this, void 0, void 0, function* () {
            const conn = yield this.acquire();
            try {
                const frame = Buffer.from([CMD_BEGIN]);
                const res = yield conn.sendFrame(frame);
                if (res.status !== STATUS_OK)
                    throw new Error(res.message);
                return new Tx(this, conn);
            }
            catch (err) {
                conn.close();
                this.release(conn);
                throw err;
            }
        });
    }
    collectionCreate(col) {
        return this.execute(buildFrame(CMD_COLLECTION_CREATE, col), (r) => ({
            Message: r.message,
        }));
    }
    collectionDelete(col) {
        return this.execute(buildFrame(CMD_COLLECTION_DELETE, col), (r) => ({
            Message: r.message,
        }));
    }
    collectionList() {
        return this.execute(Buffer.from([CMD_COLLECTION_LIST]), (r) => BSON.deserialize(r.data).list);
    }
    collectionIndexCreate(col, field) {
        return this.execute(buildFrame(CMD_COLLECTION_INDEX_CREATE, col, field), (r) => ({ Message: r.message }));
    }
    // ¡Aquí está la función que faltaba!
    collectionIndexDelete(col, field) {
        return this.execute(buildFrame(CMD_COLLECTION_INDEX_DELETE, col, field), (r) => ({ Message: r.message }));
    }
    collectionIndexList(col) {
        return this.execute(buildFrame(CMD_COLLECTION_INDEX_LIST, col), (r) => BSON.deserialize(r.data).list);
    }
    collectionItemSet(col, value, key = "") {
        const frame = buildFrame(CMD_COLLECTION_ITEM_SET, col, key, BSON.serialize(value));
        return this.execute(frame, (r) => BSON.deserialize(r.data));
    }
    collectionItemSetMany(col, items) {
        const frame = buildFrame(CMD_COLLECTION_ITEM_SET_MANY, col, BSON.serialize({ array: items }));
        return this.execute(frame, (r) => ({ Message: r.message }));
    }
    collectionItemUpdate(col, key, patch) {
        const frame = buildFrame(CMD_COLLECTION_ITEM_UPDATE, col, key, BSON.serialize(patch));
        return this.execute(frame, (r) => ({ Message: r.message }));
    }
    collectionItemDelete(col, key) {
        return this.execute(buildFrame(CMD_COLLECTION_ITEM_DELETE, col, key), (r) => ({ Message: r.message }));
    }
    collectionItemDeleteMany(col, keys) {
        return this.execute(buildFrame(CMD_COLLECTION_ITEM_DELETE_MANY, col, keys), (r) => ({ Message: r.message }));
    }
    collectionItemGet(col, key) {
        return this.execute(buildFrame(CMD_COLLECTION_ITEM_GET, col, key), (r) => {
            if (r.status === STATUS_NOT_FOUND)
                return { found: false, message: r.message, value: null };
            return {
                found: true,
                message: r.message,
                value: BSON.deserialize(r.data),
            };
        });
    }
    collectionQuery(col, query) {
        const frame = buildFrame(CMD_COLLECTION_QUERY, col, BSON.serialize(query));
        return this.execute(frame, (r) => BSON.deserialize(r.data).results);
    }
    collectionUpdateWhere(col, query, patch) {
        const frame = buildFrame(CMD_COLLECTION_UPDATE_WHERE, col, BSON.serialize(query), BSON.serialize(patch));
        return this.execute(frame, (r) => ({ Message: r.message }));
    }
    collectionDeleteWhere(col, query) {
        const frame = buildFrame(CMD_COLLECTION_DELETE_WHERE, col, BSON.serialize(query));
        return this.execute(frame, (r) => ({ Message: r.message }));
    }
}
export default LunaDBClient;
