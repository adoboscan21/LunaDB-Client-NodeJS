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

function getStatusString(status: number): string {
  const statuses: { [key: number]: string } = {
    1: "OK",
    2: "NOT_FOUND",
    3: "ERROR",
    4: "BAD_COMMAND",
    5: "UNAUTHORIZED",
    6: "BAD_REQUEST",
  };
  return statuses[status] || "UNKNOWN_STATUS";
}

export interface CommandResponse {
  status: number;
  message: string;
  data: Buffer;
}

export interface GetResult<T = any> {
  found: boolean;
  message: string;
  value: T | null;
}

export interface MutationResult {
  Message: string;
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

// 🚀 HIGH PERFORMANCE ZERO-COPY FRAME BUILDER
// Soporta Uint8Array (BSON) nativo y lo convierte a Node Buffer sin duplicar memoria
function buildFrame(
  cmd: number,
  ...args: (string | Uint8Array | string[])[]
): Buffer {
  let size = 1; // 1 byte para el comando
  const parsedArgs = args.map((arg) => {
    if (typeof arg === "string") {
      const b = Buffer.from(arg, "utf8");
      size += 4 + b.length;
      return { type: "buf", data: b };
    } else if (arg instanceof Uint8Array) {
      // ZERO-COPY CAST: Usamos el buffer de memoria subyacente directamente
      const b = Buffer.isBuffer(arg)
        ? arg
        : Buffer.from(arg.buffer, arg.byteOffset, arg.byteLength);
      size += 4 + b.length;
      return { type: "buf", data: b };
    } else if (Array.isArray(arg)) {
      size += 4; // 4 bytes para la cantidad de llaves
      const bufs = arg.map((s) => Buffer.from(s, "utf8"));
      for (const b of bufs) size += 4 + b.length;
      return { type: "arr", data: bufs };
    }
    throw new Error("Invalid argument type for frame builder");
  });

  const buf = Buffer.allocUnsafe(size);
  let offset = 0;
  buf.writeUInt8(cmd, offset++);

  for (const arg of parsedArgs) {
    if (arg.type === "buf") {
      const b = arg.data as Buffer;
      buf.writeUInt32LE(b.length, offset);
      offset += 4;
      b.copy(buf, offset);
      offset += b.length;
    } else if (arg.type === "arr") {
      const arr = arg.data as Buffer[];
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
  public socket: tls.TLSSocket | null = null;
  public isClosed: boolean = false;
  private responseBuffer = Buffer.allocUnsafe(0);
  private responseWaiter: ((value: CommandResponse) => void) | null = null;
  private errorWaiter: ((err: Error) => void) | null = null;

  public connect(
    host: string,
    port: number,
    user?: string | null,
    pass?: string | null,
    cert?: string | null,
    rejectAuth = true,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const options: tls.ConnectionOptions = {
        host,
        port,
        rejectUnauthorized: rejectAuth,
      };
      if (cert) options.ca = [fs.readFileSync(cert)];

      this.socket = tls.connect(options, async () => {
        this.socket!.on("data", (chunk) => {
          if (this.responseBuffer.length === 0) {
            this.responseBuffer = chunk;
          } else {
            this.responseBuffer = Buffer.concat([this.responseBuffer, chunk]);
          }
          this.tryProcessResponse();
        });

        this.socket!.on("close", () => {
          this.isClosed = true;
          this.triggerError(new Error("Socket closed"));
        });
        this.socket!.on("error", (err) => {
          this.isClosed = true;
          this.triggerError(err);
        });

        if (user && pass) {
          try {
            const frame = buildFrame(CMD_AUTHENTICATE, user, pass);
            const res = await this.sendFrame(frame);
            if (res.status !== STATUS_OK)
              return reject(new Error(`Auth failed: ${res.message}`));
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

    while (this.responseBuffer.length >= 5) {
      const msgLen = this.responseBuffer.readUInt32LE(1);
      const reqForDataLen = 5 + msgLen + 4;
      if (this.responseBuffer.length < reqForDataLen) return;
      const dataLen = this.responseBuffer.readUInt32LE(5 + msgLen);
      const totalLen = reqForDataLen + dataLen;
      if (this.responseBuffer.length < totalLen) return;

      const status = this.responseBuffer.readUInt8(0);
      const message = this.responseBuffer.toString("utf8", 5, 5 + msgLen);

      const data = Buffer.from(
        this.responseBuffer.subarray(reqForDataLen, totalLen),
      );

      if (this.responseBuffer.length === totalLen) {
        this.responseBuffer = Buffer.allocUnsafe(0);
      } else {
        this.responseBuffer = this.responseBuffer.subarray(totalLen);
      }

      const waiter = this.responseWaiter;
      this.responseWaiter = null;
      this.errorWaiter = null;
      waiter({ status, message, data });
      if (!this.responseWaiter) return;
    }
  }

  public async sendFrame(frame: Buffer): Promise<CommandResponse> {
    if (this.isClosed) throw new Error("Connection is dead");
    return new Promise((resolve, reject) => {
      this.responseWaiter = resolve;
      this.errorWaiter = reject;
      this.socket!.write(frame, (err) => {
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

  constructor(
    private client: LunaDBClient,
    private conn: LunaDBConnection,
  ) {}

  private async execute(frame: Buffer): Promise<CommandResponse> {
    if (this.closed) throw new Error("Transaction is already closed");
    try {
      const res = await this.conn.sendFrame(frame);
      if (res.status !== STATUS_OK) throw new Error(res.message);
      return res;
    } catch (err) {
      this.closed = true;
      this.client.release(this.conn);
      throw err;
    }
  }

  public async collectionItemSet(
    col: string,
    value: any,
    key: string = "",
  ): Promise<MutationResult> {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_SET,
      col,
      key,
      BSON.serialize(value),
    );
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionItemSetMany(
    col: string,
    items: any[],
  ): Promise<MutationResult> {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_SET_MANY,
      col,
      BSON.serialize({ array: items }),
    );
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionItemUpdate(
    col: string,
    key: string,
    patch: any,
  ): Promise<MutationResult> {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_UPDATE,
      col,
      key,
      BSON.serialize(patch),
    );
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionItemUpdateMany(
    col: string,
    items: any[],
  ): Promise<MutationResult> {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_UPDATE_MANY,
      col,
      BSON.serialize({ array: items }),
    );
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionItemDelete(
    col: string,
    key: string,
  ): Promise<MutationResult> {
    const frame = buildFrame(CMD_COLLECTION_ITEM_DELETE, col, key);
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionItemDeleteMany(
    col: string,
    keys: string[],
  ): Promise<MutationResult> {
    const frame = buildFrame(CMD_COLLECTION_ITEM_DELETE_MANY, col, keys);
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionUpdateWhere(
    col: string,
    query: Query,
    patch: any,
  ): Promise<MutationResult> {
    const frame = buildFrame(
      CMD_COLLECTION_UPDATE_WHERE,
      col,
      BSON.serialize(query),
      BSON.serialize(patch),
    );
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async collectionDeleteWhere(
    col: string,
    query: Query,
  ): Promise<MutationResult> {
    const frame = buildFrame(
      CMD_COLLECTION_DELETE_WHERE,
      col,
      BSON.serialize(query),
    );
    const res = await this.execute(frame);
    return { Message: res.message };
  }

  public async rollback(): Promise<MutationResult> {
    if (this.closed) return { Message: "Already closed" };
    const frame = Buffer.from([CMD_ROLLBACK]);
    const res = await this.conn.sendFrame(frame);
    this.closed = true;
    this.client.release(this.conn);
    return { Message: res.message };
  }

  public async commit(): Promise<MutationResult> {
    if (this.closed) throw new Error("Transaction is already closed");
    const frame = Buffer.from([CMD_COMMIT]);
    const res = await this.conn.sendFrame(frame);
    this.closed = true;
    this.client.release(this.conn);
    if (res.status !== STATUS_OK) throw new Error(res.message);
    return { Message: res.message };
  }
}

// --- Main Client (Connection Pool Manager) ---
export class LunaDBClient {
  private pool: LunaDBConnection[] = [];
  private waiters: {
    resolve: (conn: LunaDBConnection) => void;
    reject: (err: Error) => void;
  }[] = [];
  private activeConnections: number = 0;

  constructor(
    private host: string,
    private port: number,
    private username?: string,
    private password?: string,
    private serverCertPath?: string,
    private rejectUnauthorized: boolean = true,
    private poolSize: number = 100,
  ) {}

  public async connect(): Promise<void> {
    const testConn = await this.acquire();
    this.release(testConn);
  }

  public close(): void {
    for (const conn of this.pool) conn.close();
    this.pool = [];
    this.activeConnections = 0;
    for (const waiter of this.waiters)
      waiter.reject(new Error("Client closed"));
    this.waiters = [];
  }

  private async acquire(): Promise<LunaDBConnection> {
    this.pool = this.pool.filter((c) => !c.isClosed);
    if (this.pool.length > 0) return this.pool.pop()!;

    if (this.activeConnections < this.poolSize) {
      this.activeConnections++;
      const conn = new LunaDBConnection();
      try {
        await conn.connect(
          this.host,
          this.port,
          this.username,
          this.password,
          this.serverCertPath,
          this.rejectUnauthorized,
        );
        conn.socket!.once("close", () => {
          this.activeConnections--;
          this.pool = this.pool.filter((c) => c !== conn);
        });
        return conn;
      } catch (err: any) {
        this.activeConnections--;
        throw new Error(`LunaDB connection failed: ${err.message}`);
      }
    }

    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  public release(conn: LunaDBConnection): void {
    if (conn.isClosed) {
      if (this.waiters.length > 0) {
        const waiter = this.waiters.shift()!;
        this.acquire().then(waiter.resolve).catch(waiter.reject);
      }
      return;
    }
    if (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      waiter.resolve(conn);
    } else {
      this.pool.push(conn);
    }
  }

  private async execute<T>(
    frame: Buffer,
    parser: (res: CommandResponse) => T,
  ): Promise<T> {
    const conn = await this.acquire();
    try {
      const res = await conn.sendFrame(frame);
      if (res.status !== STATUS_OK && res.status !== STATUS_NOT_FOUND) {
        throw new Error(`${getStatusString(res.status)}: ${res.message}`);
      }
      this.release(conn);
      return parser(res);
    } catch (err) {
      conn.close();
      this.release(conn);
      throw err;
    }
  }

  public async begin(): Promise<Tx> {
    const conn = await this.acquire();
    try {
      const frame = Buffer.from([CMD_BEGIN]);
      const res = await conn.sendFrame(frame);
      if (res.status !== STATUS_OK) throw new Error(res.message);
      return new Tx(this, conn);
    } catch (err) {
      conn.close();
      this.release(conn);
      throw err;
    }
  }

  public collectionCreate(col: string) {
    return this.execute(buildFrame(CMD_COLLECTION_CREATE, col), (r) => ({
      Message: r.message,
    }));
  }

  public collectionDelete(col: string) {
    return this.execute(buildFrame(CMD_COLLECTION_DELETE, col), (r) => ({
      Message: r.message,
    }));
  }

  public collectionList(): Promise<string[]> {
    return this.execute(
      Buffer.from([CMD_COLLECTION_LIST]),
      (r) => BSON.deserialize(r.data).list as string[],
    );
  }

  public collectionIndexCreate(col: string, field: string) {
    return this.execute(
      buildFrame(CMD_COLLECTION_INDEX_CREATE, col, field),
      (r) => ({ Message: r.message }),
    );
  }

  // ¡Aquí está la función que faltaba!
  public collectionIndexDelete(col: string, field: string) {
    return this.execute(
      buildFrame(CMD_COLLECTION_INDEX_DELETE, col, field),
      (r) => ({ Message: r.message }),
    );
  }

  public collectionIndexList(col: string): Promise<string[]> {
    return this.execute(
      buildFrame(CMD_COLLECTION_INDEX_LIST, col),
      (r) => BSON.deserialize(r.data).list as string[],
    );
  }

  public collectionItemSet<T = any>(
    col: string,
    value: T,
    key: string = "",
  ): Promise<T> {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_SET,
      col,
      key,
      BSON.serialize(value as any),
    );
    return this.execute(frame, (r) => BSON.deserialize(r.data) as T);
  }

  public collectionItemSetMany(col: string, items: any[]) {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_SET_MANY,
      col,
      BSON.serialize({ array: items }),
    );
    return this.execute(frame, (r) => ({ Message: r.message }));
  }

  public collectionItemUpdate(col: string, key: string, patch: any) {
    const frame = buildFrame(
      CMD_COLLECTION_ITEM_UPDATE,
      col,
      key,
      BSON.serialize(patch),
    );
    return this.execute(frame, (r) => ({ Message: r.message }));
  }

  public collectionItemDelete(col: string, key: string) {
    return this.execute(
      buildFrame(CMD_COLLECTION_ITEM_DELETE, col, key),
      (r) => ({ Message: r.message }),
    );
  }

  public collectionItemDeleteMany(col: string, keys: string[]) {
    return this.execute(
      buildFrame(CMD_COLLECTION_ITEM_DELETE_MANY, col, keys),
      (r) => ({ Message: r.message }),
    );
  }

  public collectionItemGet<T = any>(
    col: string,
    key: string,
  ): Promise<GetResult<T>> {
    return this.execute(buildFrame(CMD_COLLECTION_ITEM_GET, col, key), (r) => {
      if (r.status === STATUS_NOT_FOUND)
        return { found: false, message: r.message, value: null };
      return {
        found: true,
        message: r.message,
        value: BSON.deserialize(r.data) as T,
      };
    });
  }

  public collectionQuery<T = any>(col: string, query: Query): Promise<T> {
    const frame = buildFrame(CMD_COLLECTION_QUERY, col, BSON.serialize(query));
    return this.execute(frame, (r) => BSON.deserialize(r.data).results as T);
  }

  public collectionUpdateWhere(col: string, query: Query, patch: any) {
    const frame = buildFrame(
      CMD_COLLECTION_UPDATE_WHERE,
      col,
      BSON.serialize(query),
      BSON.serialize(patch),
    );
    return this.execute(frame, (r) => ({ Message: r.message }));
  }

  public collectionDeleteWhere(col: string, query: Query) {
    const frame = buildFrame(
      CMD_COLLECTION_DELETE_WHERE,
      col,
      BSON.serialize(query),
    );
    return this.execute(frame, (r) => ({ Message: r.message }));
  }
}

export default LunaDBClient;
