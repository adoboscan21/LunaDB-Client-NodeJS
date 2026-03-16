// lunadb-client.d.ts
declare module "lunadb-client" {
  import * as tls from "node:tls";

  // --- Interfaces for results---
  export interface GetResult<T = any> {
    found: boolean;
    message: string;
    value: T | null;
  }

  // --- Interfaces for queries ---
  export interface OrderByClause {
    field: string;
    direction: "asc" | "desc";
  }

  export interface Aggregation {
    func: "sum" | "avg" | "min" | "max" | "count";
    field: string;
  }

  export interface LookupClause {
    from: string;
    localField: string;
    foreignField: string;
    as: string;
  }

  export interface Query {
    filter?: { [key: string]: any };
    order_by?: OrderByClause[];
    limit?: number;
    offset?: number;
    count?: boolean;
    aggregations?: { [key: string]: Aggregation };
    group_by?: string[];
    having?: { [key: string]: any };
    distinct?: string;
    projection?: string[];
    lookups?: LookupClause[];
  }

  // --- Transaction Class ---
  export class Tx {
    public collectionItemSet<T = any>(collectionName: string, value: T, key?: string): Promise<string>;
    public collectionItemSetMany<T extends { _id?: string }>(collectionName: string, items: T[]): Promise<string>;
    public collectionItemUpdate<T = any>(collectionName: string, key: string, patchValue: Partial<T>): Promise<string>;
    public collectionItemUpdateMany<T = any>(collectionName: string, items: { _id: string; patch: Partial<T> }[]): Promise<string>;
    public collectionItemDelete(collectionName: string, key: string): Promise<string>;
    public collectionItemDeleteMany(collectionName: string, keys: string[]): Promise<string>;
    public commit(): Promise<string>;
    public rollback(): Promise<string>;
  }

  // --- Main Client Class ---
  export class LunaDBClient {
    constructor(
      host: string,
      port: number,
      username?: string,
      password?: string,
      serverCertPath?: string,
      rejectUnauthorized?: boolean,
      poolSize?: number
    );

    public readonly isAuthenticatedSession: boolean;
    public readonly authenticatedUser: string | null;

    public connect(): Promise<void>;
    public close(): void;

    public begin(): Promise<Tx>;

    public collectionCreate(collectionName: string): Promise<string>;
    public collectionDelete(collectionName: string): Promise<string>;
    public collectionList(): Promise<string[]>;

    public collectionIndexCreate(collectionName: string, fieldName: string): Promise<string>;
    public collectionIndexDelete(collectionName: string, fieldName: string): Promise<string>;
    public collectionIndexList(collectionName: string): Promise<string[]>;

    public collectionItemSet<T = any>(collectionName: string, value: T, key?: string): Promise<T>;
    public collectionItemSetMany<T extends { _id?: string }>(collectionName: string, items: T[]): Promise<string>;
    public collectionItemUpdate<T = any>(collectionName: string, key: string, patchValue: Partial<T>): Promise<string>;
    public collectionItemUpdateMany<T = any>(collectionName: string, items: { _id: string; patch: Partial<T> }[]): Promise<string>;
    public collectionItemGet<T = any>(collectionName: string, key: string): Promise<GetResult<T>>;
    public collectionItemDelete(collectionName: string, key: string): Promise<string>;
    public collectionItemDeleteMany(collectionName: string, keys: string[]): Promise<string>;
    public collectionQuery<T = any>(collectionName: string, query: Query): Promise<T>;
  }

  export default LunaDBClient;
}