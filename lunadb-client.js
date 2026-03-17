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
// Collection Management Commands
const CMD_COLLECTION_CREATE = 1;
const CMD_COLLECTION_DELETE = 2;
const CMD_COLLECTION_LIST = 3;
const CMD_COLLECTION_INDEX_CREATE = 4;
const CMD_COLLECTION_INDEX_DELETE = 5;
const CMD_COLLECTION_INDEX_LIST = 6;
// Collection Item Commands
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
// Authentication Commands
const CMD_AUTHENTICATE = 18;
// Transaction Commands
const CMD_BEGIN = 25;
const CMD_COMMIT = 26;
const CMD_ROLLBACK = 27;
// --- Status Codes ---
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
function writeString(str) {
    const strBuffer = Buffer.from(str, "utf8");
    const lenBuffer = Buffer.alloc(4);
    lenBuffer.writeUInt32LE(strBuffer.length, 0);
    return Buffer.concat([lenBuffer, strBuffer]);
}
function writeBytes(bytes) {
    const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    const lenBuffer = Buffer.alloc(4);
    lenBuffer.writeUInt32LE(buf.length, 0);
    return Buffer.concat([lenBuffer, buf]);
}
// --- Internal Connection Wrapper ---
class LunaDBConnection {
    constructor() {
        this.socket = null;
        this.isClosed = false; // Tracks socket health
        this.responseBuffer = Buffer.alloc(0);
        this.responseWaiter = null;
        this.errorWaiter = null;
    }
    connect(host, port, user, pass, cert, rejectAuth = true) {
        return new Promise((resolve, reject) => {
            const options = { host, port, rejectUnauthorized: rejectAuth };
            if (cert)
                options.ca = [fs.readFileSync(cert)];
            this.socket = tls.connect(options, () => __awaiter(this, void 0, void 0, function* () {
                this.socket.on("data", (chunk) => {
                    this.responseBuffer = Buffer.concat([this.responseBuffer, chunk]);
                    this.tryProcessResponse();
                });
                this.socket.on("close", () => { this.isClosed = true; this.triggerError(new Error("Socket closed by remote peer")); });
                this.socket.on("error", (err) => { this.isClosed = true; this.triggerError(err); });
                if (user && pass) {
                    try {
                        const payload = Buffer.concat([writeString(user), writeString(pass)]);
                        const res = yield this.sendCommand(CMD_AUTHENTICATE, payload);
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
        while (true) {
            if (this.responseBuffer.length < 5)
                return;
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
            const data = this.responseBuffer.subarray(reqForDataLen, totalLen);
            this.responseBuffer = this.responseBuffer.subarray(totalLen);
            const waiter = this.responseWaiter;
            this.responseWaiter = null;
            this.errorWaiter = null;
            waiter({ status, message, data });
            if (!this.responseWaiter)
                return;
        }
    }
    sendCommand(cmd, payload) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.isClosed)
                throw new Error("Cannot send command: Connection is dead");
            return new Promise((resolve, reject) => {
                this.responseWaiter = resolve;
                this.errorWaiter = reject;
                this.socket.write(Buffer.concat([Buffer.from([cmd]), payload]), (err) => {
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
    execute(cmd, payload) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closed)
                throw new Error("Transaction is already closed");
            try {
                const res = yield this.conn.sendCommand(cmd, payload);
                if (res.status !== STATUS_OK)
                    throw new Error(res.message);
                return res;
            }
            catch (err) {
                this.closed = true;
                this.client.release(this.conn); // Release on error (server will auto-rollback dead connections)
                throw err;
            }
        });
    }
    collectionItemSet(col_1, value_1) {
        return __awaiter(this, arguments, void 0, function* (col, value, key = "") {
            const payload = Buffer.concat([
                writeString(col),
                writeString(key),
                writeBytes(BSON.serialize(value))
            ]);
            const res = yield this.execute(CMD_COLLECTION_ITEM_SET, payload);
            return res.message;
        });
    }
    collectionItemSetMany(col, items) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
            const res = yield this.execute(CMD_COLLECTION_ITEM_SET_MANY, payload);
            return res.message;
        });
    }
    collectionItemUpdate(col, key, patch) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = Buffer.concat([writeString(col), writeString(key), writeBytes(BSON.serialize(patch))]);
            const res = yield this.execute(CMD_COLLECTION_ITEM_UPDATE, payload);
            return res.message;
        });
    }
    collectionItemUpdateMany(col, items) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
            const res = yield this.execute(CMD_COLLECTION_ITEM_UPDATE_MANY, payload);
            return res.message;
        });
    }
    collectionItemDelete(col, key) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = Buffer.concat([writeString(col), writeString(key)]);
            const res = yield this.execute(CMD_COLLECTION_ITEM_DELETE, payload);
            return res.message;
        });
    }
    collectionItemDeleteMany(col, keys) {
        return __awaiter(this, void 0, void 0, function* () {
            const keysCountBuffer = Buffer.alloc(4);
            keysCountBuffer.writeUInt32LE(keys.length, 0);
            const keysPayload = keys.map(k => writeString(k));
            const payload = Buffer.concat([writeString(col), keysCountBuffer, ...keysPayload]);
            const res = yield this.execute(CMD_COLLECTION_ITEM_DELETE_MANY, payload);
            return res.message;
        });
    }
    collectionUpdateWhere(col, query, patch) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = Buffer.concat([
                writeString(col),
                writeBytes(BSON.serialize(query)),
                writeBytes(BSON.serialize(patch))
            ]);
            const res = yield this.execute(CMD_COLLECTION_UPDATE_WHERE, payload);
            return res.message;
        });
    }
    collectionDeleteWhere(col, query) {
        return __awaiter(this, void 0, void 0, function* () {
            const payload = Buffer.concat([
                writeString(col),
                writeBytes(BSON.serialize(query))
            ]);
            const res = yield this.execute(CMD_COLLECTION_DELETE_WHERE, payload);
            return res.message;
        });
    }
    rollback() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closed)
                return "Already closed";
            const res = yield this.conn.sendCommand(CMD_ROLLBACK, Buffer.alloc(0));
            this.closed = true;
            this.client.release(this.conn);
            return res.message;
        });
    }
    commit() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closed)
                throw new Error("Transaction is already closed");
            const res = yield this.conn.sendCommand(CMD_COMMIT, Buffer.alloc(0));
            this.closed = true;
            this.client.release(this.conn);
            if (res.status !== STATUS_OK)
                throw new Error(res.message);
            return res.message;
        });
    }
}
// --- Main Client (Connection Pool Manager) ---
export class LunaDBClient {
    constructor(host, port, username, password, serverCertPath, rejectUnauthorized = true, poolSize = 10 // Sensible default
    ) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.serverCertPath = serverCertPath;
        this.rejectUnauthorized = rejectUnauthorized;
        this.poolSize = poolSize;
        this.pool = [];
        this.waiters = [];
    }
    get isAuthenticatedSession() {
        return !!this.username;
    }
    get authenticatedUser() {
        return this.username || null;
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            const promises = [];
            for (let i = 0; i < this.poolSize; i++) {
                promises.push(this.createNewConnection());
            }
            yield Promise.all(promises);
        });
    }
    createNewConnection() {
        return __awaiter(this, void 0, void 0, function* () {
            const conn = new LunaDBConnection();
            yield conn.connect(this.host, this.port, this.username, this.password, this.serverCertPath, this.rejectUnauthorized);
            this.pool.push(conn);
            return conn;
        });
    }
    close() {
        for (const conn of this.pool)
            conn.close();
        this.pool = [];
    }
    acquire() {
        return __awaiter(this, void 0, void 0, function* () {
            while (this.pool.length > 0) {
                const conn = this.pool.pop();
                if (!conn.isClosed)
                    return conn; // Toss out dead connections
            }
            return new Promise(resolve => this.waiters.push(resolve));
        });
    }
    release(conn) {
        if (conn.isClosed) {
            // Heal the pool asynchronously if the connection died
            this.createNewConnection().catch(() => { });
            return;
        }
        if (this.waiters.length > 0) {
            const resolve = this.waiters.shift();
            resolve(conn);
        }
        else {
            this.pool.push(conn);
        }
    }
    execute(cmd, payload, parser) {
        return __awaiter(this, void 0, void 0, function* () {
            const conn = yield this.acquire();
            try {
                const res = yield conn.sendCommand(cmd, payload);
                if (res.status !== STATUS_OK && res.status !== STATUS_NOT_FOUND) {
                    throw new Error(`${getStatusString(res.status)}: ${res.message}`);
                }
                this.release(conn);
                return parser(res);
            }
            catch (err) {
                conn.close(); // Force kill on error to prevent protocol desync
                this.release(conn); // Release will trigger self-healing
                throw err;
            }
        });
    }
    begin() {
        return __awaiter(this, void 0, void 0, function* () {
            const conn = yield this.acquire();
            try {
                const res = yield conn.sendCommand(CMD_BEGIN, Buffer.alloc(0));
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
        return this.execute(CMD_COLLECTION_CREATE, writeString(col), r => r.message);
    }
    collectionDelete(col) {
        return this.execute(CMD_COLLECTION_DELETE, writeString(col), r => r.message);
    }
    collectionList() {
        return this.execute(CMD_COLLECTION_LIST, Buffer.alloc(0), r => {
            return BSON.deserialize(r.data).list;
        });
    }
    collectionIndexCreate(col, field) {
        return this.execute(CMD_COLLECTION_INDEX_CREATE, Buffer.concat([writeString(col), writeString(field)]), r => r.message);
    }
    collectionIndexDelete(col, field) {
        return this.execute(CMD_COLLECTION_INDEX_DELETE, Buffer.concat([writeString(col), writeString(field)]), r => r.message);
    }
    collectionIndexList(col) {
        return this.execute(CMD_COLLECTION_INDEX_LIST, writeString(col), r => {
            return BSON.deserialize(r.data).list;
        });
    }
    collectionItemSet(col, value, key = "") {
        const payload = Buffer.concat([
            writeString(col),
            writeString(key),
            writeBytes(BSON.serialize(value))
        ]);
        return this.execute(CMD_COLLECTION_ITEM_SET, payload, r => BSON.deserialize(r.data));
    }
    collectionItemSetMany(col, items) {
        const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
        return this.execute(CMD_COLLECTION_ITEM_SET_MANY, payload, r => r.message);
    }
    collectionItemUpdate(col, key, patch) {
        const payload = Buffer.concat([writeString(col), writeString(key), writeBytes(BSON.serialize(patch))]);
        return this.execute(CMD_COLLECTION_ITEM_UPDATE, payload, r => r.message);
    }
    collectionItemUpdateMany(col, items) {
        const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
        return this.execute(CMD_COLLECTION_ITEM_UPDATE_MANY, payload, r => r.message);
    }
    collectionItemDelete(col, key) {
        return this.execute(CMD_COLLECTION_ITEM_DELETE, Buffer.concat([writeString(col), writeString(key)]), r => r.message);
    }
    collectionItemDeleteMany(col, keys) {
        const keysCountBuffer = Buffer.alloc(4);
        keysCountBuffer.writeUInt32LE(keys.length, 0);
        const keysPayload = keys.map(k => writeString(k));
        const payload = Buffer.concat([writeString(col), keysCountBuffer, ...keysPayload]);
        return this.execute(CMD_COLLECTION_ITEM_DELETE_MANY, payload, r => r.message);
    }
    collectionItemGet(col, key) {
        return this.execute(CMD_COLLECTION_ITEM_GET, Buffer.concat([writeString(col), writeString(key)]), r => {
            if (r.status === STATUS_NOT_FOUND)
                return { found: false, message: r.message, value: null };
            return { found: true, message: r.message, value: BSON.deserialize(r.data) };
        });
    }
    collectionQuery(col, query) {
        const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize(query))]);
        return this.execute(CMD_COLLECTION_QUERY, payload, r => BSON.deserialize(r.data).results);
    }
    collectionUpdateWhere(col, query, patch) {
        const payload = Buffer.concat([
            writeString(col),
            writeBytes(BSON.serialize(query)),
            writeBytes(BSON.serialize(patch))
        ]);
        return this.execute(CMD_COLLECTION_UPDATE_WHERE, payload, r => r.message);
    }
    collectionDeleteWhere(col, query) {
        const payload = Buffer.concat([
            writeString(col),
            writeBytes(BSON.serialize(query))
        ]);
        return this.execute(CMD_COLLECTION_DELETE_WHERE, payload, r => r.message);
    }
}
export default LunaDBClient;
