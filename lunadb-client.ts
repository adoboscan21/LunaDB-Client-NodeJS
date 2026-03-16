import tls from "node:tls";
import fs from "node:fs";
import { Buffer } from "node:buffer";
import { BSON } from "bson";

// --- Protocol Constants ---
const CMD_COLLECTION_CREATE = 3;
const CMD_COLLECTION_DELETE = 4;
const CMD_COLLECTION_LIST = 5;
const CMD_COLLECTION_INDEX_CREATE = 6;
const CMD_COLLECTION_INDEX_DELETE = 7;
const CMD_COLLECTION_INDEX_LIST = 8;
const CMD_COLLECTION_ITEM_SET = 9;
const CMD_COLLECTION_ITEM_SET_MANY = 10;
const CMD_COLLECTION_ITEM_GET = 11;
const CMD_COLLECTION_ITEM_DELETE = 12;
const CMD_COLLECTION_QUERY = 14;
const CMD_COLLECTION_ITEM_DELETE_MANY = 15;
const CMD_COLLECTION_ITEM_UPDATE = 16;
const CMD_COLLECTION_ITEM_UPDATE_MANY = 17;
const CMD_AUTHENTICATE = 18;
const CMD_BEGIN = 25;
const CMD_COMMIT = 26;
const CMD_ROLLBACK = 27;

const STATUS_OK = 1;
const STATUS_NOT_FOUND = 2;

function getStatusString(status: number): string {
  const statuses: { [key: number]: string } = {
    1: "OK", 2: "NOT_FOUND", 3: "ERROR", 4: "BAD_COMMAND", 5: "UNAUTHORIZED", 6: "BAD_REQUEST",
  };
  return statuses[status] || "UNKNOWN_STATUS";
}

interface CommandResponse {
  status: number;
  message: string;
  data: Buffer;
}

export interface GetResult<T = any> {
  found: boolean;
  message: string;
  value: T | null;
}

export interface Query {
  filter?: any;
  order_by?: { field: string; direction: "asc" | "desc" }[];
  limit?: number;
  offset?: number;
  count?: boolean;
  aggregations?: any;
  group_by?: string[];
  having?: any;
  distinct?: string;
  projection?: string[];
  lookups?: any[];
}

function writeString(str: string): Buffer {
  const strBuffer = Buffer.from(str, "utf8");
  const lenBuffer = Buffer.alloc(4);
  lenBuffer.writeUInt32LE(strBuffer.length, 0);
  return Buffer.concat([lenBuffer, strBuffer]);
}

function writeBytes(bytes: Buffer | Uint8Array): Buffer {
  const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
  const lenBuffer = Buffer.alloc(4);
  lenBuffer.writeUInt32LE(buf.length, 0);
  return Buffer.concat([lenBuffer, buf]);
}

// --- Internal Connection Wrapper ---
class LunaDBConnection {
  public socket: tls.TLSSocket | null = null;
  public isClosed: boolean = false; // Tracks socket health
  private responseBuffer = Buffer.alloc(0);
  private responseWaiter: ((value: CommandResponse) => void) | null = null;
  private errorWaiter: ((err: Error) => void) | null = null;

  public connect(host: string, port: number, user?: string | null, pass?: string | null, cert?: string | null, rejectAuth = true): Promise<void> {
    return new Promise((resolve, reject) => {
      const options: tls.ConnectionOptions = { host, port, rejectUnauthorized: rejectAuth };
      if (cert) options.ca = [fs.readFileSync(cert)];

      this.socket = tls.connect(options, async () => {
        this.socket!.on("data", (chunk) => {
          this.responseBuffer = Buffer.concat([this.responseBuffer, chunk]);
          this.tryProcessResponse();
        });

        this.socket!.on("close", () => { this.isClosed = true; this.triggerError(new Error("Socket closed by remote peer")); });
        this.socket!.on("error", (err) => { this.isClosed = true; this.triggerError(err); });

        if (user && pass) {
          try {
            const payload = Buffer.concat([writeString(user), writeString(pass)]);
            const res = await this.sendCommand(CMD_AUTHENTICATE, payload);
            if (res.status !== STATUS_OK) return reject(new Error(`Auth failed: ${res.message}`));
          } catch (e) {
            return reject(e);
          }
        }
        resolve();
      });
      this.socket.on("error", reject);
    });
  }

  private triggerError(err: Error) {
    if (this.errorWaiter) {
      this.errorWaiter(err);
      this.responseWaiter = null;
      this.errorWaiter = null;
    }
  }

  private tryProcessResponse(): void {
    if (!this.responseWaiter) return;
    while (true) {
      if (this.responseBuffer.length < 5) return;
      const msgLen = this.responseBuffer.readUInt32LE(1);
      const reqForDataLen = 5 + msgLen + 4;
      if (this.responseBuffer.length < reqForDataLen) return;
      const dataLen = this.responseBuffer.readUInt32LE(5 + msgLen);
      const totalLen = reqForDataLen + dataLen;
      if (this.responseBuffer.length < totalLen) return;

      const status = this.responseBuffer.readUInt8(0);
      const message = this.responseBuffer.toString("utf8", 5, 5 + msgLen);
      const data = this.responseBuffer.subarray(reqForDataLen, totalLen);
      this.responseBuffer = this.responseBuffer.subarray(totalLen);

      const waiter = this.responseWaiter;
      this.responseWaiter = null;
      this.errorWaiter = null;
      waiter({ status, message, data });
      if (!this.responseWaiter) return;
    }
  }

  public async sendCommand(cmd: number, payload: Buffer): Promise<CommandResponse> {
    if (this.isClosed) throw new Error("Cannot send command: Connection is dead");

    return new Promise((resolve, reject) => {
      this.responseWaiter = resolve;
      this.errorWaiter = reject;

      this.socket!.write(Buffer.concat([Buffer.from([cmd]), payload]), (err) => {
        if (err) {
          this.isClosed = true;
          this.triggerError(err);
        }
      });
    });
  }

  public close() {
    this.isClosed = true;
    this.socket?.destroy();
  }
}

// --- Stateful Transaction Class ---
export class Tx {
  private closed = false;

  constructor(private client: LunaDBClient, private conn: LunaDBConnection) { }

  private async execute(cmd: number, payload: Buffer): Promise<CommandResponse> {
    if (this.closed) throw new Error("Transaction is already closed");
    try {
      const res = await this.conn.sendCommand(cmd, payload);
      if (res.status !== STATUS_OK) throw new Error(res.message);
      return res;
    } catch (err) {
      this.closed = true;
      this.client.release(this.conn); // Release on error (server will auto-rollback dead connections)
      throw err;
    }
  }

  public async collectionItemSet(col: string, value: any, key: string = ""): Promise<string> {
    const payload = Buffer.concat([
      writeString(col),
      writeString(key),
      writeBytes(BSON.serialize(value))
    ]);
    const res = await this.execute(CMD_COLLECTION_ITEM_SET, payload);
    return res.message;
  }

  public async collectionItemSetMany(col: string, items: any[]): Promise<string> {
    const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
    const res = await this.execute(CMD_COLLECTION_ITEM_SET_MANY, payload);
    return res.message;
  }

  public async collectionItemUpdate(col: string, key: string, patch: any): Promise<string> {
    const payload = Buffer.concat([writeString(col), writeString(key), writeBytes(BSON.serialize(patch))]);
    const res = await this.execute(CMD_COLLECTION_ITEM_UPDATE, payload);
    return res.message;
  }

  public async collectionItemUpdateMany(col: string, items: any[]): Promise<string> {
    const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
    const res = await this.execute(CMD_COLLECTION_ITEM_UPDATE_MANY, payload);
    return res.message;
  }

  public async collectionItemDelete(col: string, key: string): Promise<string> {
    const payload = Buffer.concat([writeString(col), writeString(key)]);
    const res = await this.execute(CMD_COLLECTION_ITEM_DELETE, payload);
    return res.message;
  }

  public async collectionItemDeleteMany(col: string, keys: string[]): Promise<string> {
    const keysCountBuffer = Buffer.alloc(4);
    keysCountBuffer.writeUInt32LE(keys.length, 0);
    const keysPayload = keys.map(k => writeString(k));
    const payload = Buffer.concat([writeString(col), keysCountBuffer, ...keysPayload]);
    const res = await this.execute(CMD_COLLECTION_ITEM_DELETE_MANY, payload);
    return res.message;
  }

  public async rollback(): Promise<string> {
    if (this.closed) return "Already closed";
    const res = await this.conn.sendCommand(CMD_ROLLBACK, Buffer.alloc(0));
    this.closed = true;
    this.client.release(this.conn);
    return res.message;
  }

  public async commit(): Promise<string> {
    if (this.closed) throw new Error("Transaction is already closed");
    const res = await this.conn.sendCommand(CMD_COMMIT, Buffer.alloc(0));
    this.closed = true;
    this.client.release(this.conn);
    if (res.status !== STATUS_OK) throw new Error(res.message);
    return res.message;
  }
}

// --- Main Client (Connection Pool Manager) ---
export class LunaDBClient {
  private pool: LunaDBConnection[] = [];
  private waiters: ((conn: LunaDBConnection) => void)[] = [];

  constructor(
    private host: string,
    private port: number,
    private username?: string,
    private password?: string,
    private serverCertPath?: string,
    private rejectUnauthorized: boolean = true,
    private poolSize: number = 10 // Sensible default
  ) { }

  public get isAuthenticatedSession(): boolean {
    return !!this.username;
  }

  public get authenticatedUser(): string | null {
    return this.username || null;
  }

  public async connect(): Promise<void> {
    const promises = [];
    for (let i = 0; i < this.poolSize; i++) {
      promises.push(this.createNewConnection());
    }
    await Promise.all(promises);
  }

  private async createNewConnection(): Promise<LunaDBConnection> {
    const conn = new LunaDBConnection();
    await conn.connect(this.host, this.port, this.username, this.password, this.serverCertPath, this.rejectUnauthorized);
    this.pool.push(conn);
    return conn;
  }

  public close(): void {
    for (const conn of this.pool) conn.close();
    this.pool = [];
  }

  private async acquire(): Promise<LunaDBConnection> {
    while (this.pool.length > 0) {
      const conn = this.pool.pop()!;
      if (!conn.isClosed) return conn; // Toss out dead connections
    }
    return new Promise(resolve => this.waiters.push(resolve));
  }

  public release(conn: LunaDBConnection): void {
    if (conn.isClosed) {
      // Heal the pool asynchronously if the connection died
      this.createNewConnection().catch(() => { });
      return;
    }

    if (this.waiters.length > 0) {
      const resolve = this.waiters.shift()!;
      resolve(conn);
    } else {
      this.pool.push(conn);
    }
  }

  private async execute<T>(cmd: number, payload: Buffer, parser: (res: CommandResponse) => T): Promise<T> {
    const conn = await this.acquire();
    try {
      const res = await conn.sendCommand(cmd, payload);
      if (res.status !== STATUS_OK && res.status !== STATUS_NOT_FOUND) {
        throw new Error(`${getStatusString(res.status)}: ${res.message}`);
      }
      this.release(conn);
      return parser(res);
    } catch (err) {
      conn.close(); // Force kill on error to prevent protocol desync
      this.release(conn); // Release will trigger self-healing
      throw err;
    }
  }

  public async begin(): Promise<Tx> {
    const conn = await this.acquire();
    try {
      const res = await conn.sendCommand(CMD_BEGIN, Buffer.alloc(0));
      if (res.status !== STATUS_OK) throw new Error(res.message);
      return new Tx(this, conn);
    } catch (err) {
      conn.close();
      this.release(conn);
      throw err;
    }
  }

  public collectionCreate(col: string) {
    return this.execute(CMD_COLLECTION_CREATE, writeString(col), r => r.message);
  }

  public collectionDelete(col: string) {
    return this.execute(CMD_COLLECTION_DELETE, writeString(col), r => r.message);
  }

  public collectionList(): Promise<string[]> {
    return this.execute(CMD_COLLECTION_LIST, Buffer.alloc(0), r => {
      return BSON.deserialize(r.data).list as string[];
    });
  }

  public collectionIndexCreate(col: string, field: string) {
    return this.execute(CMD_COLLECTION_INDEX_CREATE, Buffer.concat([writeString(col), writeString(field)]), r => r.message);
  }

  public collectionIndexDelete(col: string, field: string) {
    return this.execute(CMD_COLLECTION_INDEX_DELETE, Buffer.concat([writeString(col), writeString(field)]), r => r.message);
  }

  public collectionIndexList(col: string): Promise<string[]> {
    return this.execute(CMD_COLLECTION_INDEX_LIST, writeString(col), r => {
      return BSON.deserialize(r.data).list as string[];
    });
  }

  public collectionItemSet<T = any>(col: string, value: T, key: string = ""): Promise<T> {
    const payload = Buffer.concat([
      writeString(col),
      writeString(key),
      writeBytes(BSON.serialize(value as any))
    ]);
    return this.execute(CMD_COLLECTION_ITEM_SET, payload, r => BSON.deserialize(r.data) as T);
  }

  public collectionItemSetMany(col: string, items: any[]) {
    const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
    return this.execute(CMD_COLLECTION_ITEM_SET_MANY, payload, r => r.message);
  }

  public collectionItemUpdate(col: string, key: string, patch: any) {
    const payload = Buffer.concat([writeString(col), writeString(key), writeBytes(BSON.serialize(patch))]);
    return this.execute(CMD_COLLECTION_ITEM_UPDATE, payload, r => r.message);
  }

  public collectionItemUpdateMany(col: string, items: any[]) {
    const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize({ array: items }))]);
    return this.execute(CMD_COLLECTION_ITEM_UPDATE_MANY, payload, r => r.message);
  }

  public collectionItemDelete(col: string, key: string) {
    return this.execute(CMD_COLLECTION_ITEM_DELETE, Buffer.concat([writeString(col), writeString(key)]), r => r.message);
  }

  public collectionItemDeleteMany(col: string, keys: string[]) {
    const keysCountBuffer = Buffer.alloc(4);
    keysCountBuffer.writeUInt32LE(keys.length, 0);
    const keysPayload = keys.map(k => writeString(k));
    const payload = Buffer.concat([writeString(col), keysCountBuffer, ...keysPayload]);
    return this.execute(CMD_COLLECTION_ITEM_DELETE_MANY, payload, r => r.message);
  }

  public collectionItemGet<T = any>(col: string, key: string): Promise<GetResult<T>> {
    return this.execute(CMD_COLLECTION_ITEM_GET, Buffer.concat([writeString(col), writeString(key)]), r => {
      if (r.status === STATUS_NOT_FOUND) return { found: false, message: r.message, value: null };
      return { found: true, message: r.message, value: BSON.deserialize(r.data) as T };
    });
  }

  public collectionQuery<T = any>(col: string, query: Query): Promise<T> {
    const payload = Buffer.concat([writeString(col), writeBytes(BSON.serialize(query))]);
    return this.execute(CMD_COLLECTION_QUERY, payload, r => BSON.deserialize(r.data).results as T);
  }
}

export default LunaDBClient;