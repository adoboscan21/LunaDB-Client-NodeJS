# LunaDB Node.js Client 🐈

An asynchronous, connection-pooled Node.js client for interacting with the **LunaDB** database via its secure, TLS-based BSON binary protocol. This library is designed to be highly concurrent, robust, and ergonomically friendly for TypeScript and JavaScript developers.

---

## 📖 Getting Started

### Installation

```bash
npm install lunadb-client bson
````

_(Note: `bson` is a required peer dependency as LunaDB communicates using native binary BSON for maximum performance)._

### Connection and Basic Operations

The `LunaDBClient` class automatically manages a **Connection Pool** (default: 100 sockets) under the hood to handle massive concurrency without blocking the Node.js event loop.

```JavaScript
import { LunaDBClient } from "lunadb-client";

// Server Configuration
const DB_HOST = "127.0.0.1";
const DB_PORT = 5876;
const DB_USER = "admin";
const DB_PASS = "adminpass";

// Initialize the client
// Parameters: host, port, user, pass, certPath, rejectUnauthorized, poolSize
const client = new LunaDBClient(
  DB_HOST,
  DB_PORT,
  DB_USER,
  DB_PASS,
  null,  // serverCertPath (Provide your CA cert in production)
  false, // rejectUnauthorized (Set to true in production)
  100    // poolSize (Number of concurrent TLS sockets)
);

async function runExample() {
  try {
    // 1. Establish the connection pool
    await client.connect();
    console.log(`✔ Connected and authenticated as: ${client.authenticatedUser}`);

    const collName = "users";
    await client.collectionCreate(collName);
    console.log(`✔ Collection '${collName}' created.`);

    // 2. Set an item (Auto-generates UUID if key is omitted)
    const userValue = { name: "Elena", city: "Madrid", active: true };
    const savedUser = await client.collectionItemSet(collName, userValue);
    console.log(`✔ User set successfully with ID: ${savedUser._id}`);

    // 3. Set an item with a specific explicit key
    const specificKey = "user_marcos";
    await client.collectionItemSet(
      collName,
      { name: "Marcos", city: "Bogota", active: true },
      specificKey
    );
    console.log(`✔ User '${specificKey}' set successfully.`);

    // 4. Retrieve the item back
    const result = await client.collectionItemGet(collName, specificKey);
    if (result.found) {
      console.log("✔ Item retrieved:", result.value);
    }
  } catch (error) {
    console.error("✖ Connection or operation error:", error.message);
  } finally {
    // Always close the pool when the application shuts down
    client.close();
    console.log("Connection pool closed.");
  }
}

runExample();
```

### Stateful ACID Transactions

For operations that must either all succeed or all fail, LunaDB supports strict ACID transactions. Calling `begin()` locks a dedicated connection from the pool for your transaction block.

```JavaScript
async function transactionExample() {
  await client.connect();

  // Start a transaction (returns a dedicated Tx object)
  const tx = await client.begin();
  
  try {
    // Perform operations using the 'tx' instance, NOT the 'client' instance
    await tx.collectionItemUpdate("accounts", "acc1", { balance: 50 });
    await tx.collectionItemUpdate("accounts", "acc2", { balance: 150 });

    // If all operations succeed, commit the changes to disk
    await tx.commit();
    console.log("✔ Transaction committed successfully.");
  } catch (error) {
    // If any operation fails, roll back all changes safely
    await tx.rollback();
    console.error("✖ Transaction failed, changes were rolled back.");
  } finally {
    client.close();
  }
}
```

---

## 🎯 Massive Server-Side Mutations

Update or delete thousands of records instantly using a query filter. The data never travels over the network; the LunaDB engine handles the B-Tree lookups and physical batching entirely internally.

```JavaScript
// Update Where: Set status to 'Shipped' for all 'Pending' orders
const updateQuery = {
  filter: { field: "status", op: "=", value: "Pending" }
};
const patch = { status: "Shipped" };
await client.collectionUpdateWhere("orders", updateQuery, patch);
console.log("✔ All pending orders updated to shipped.");

// Delete Where: Delete all 'Cancelled' orders
const deleteQuery = {
  filter: { field: "status", op: "=", value: "Cancelled" }
};
await client.collectionDeleteWhere("orders", deleteQuery);
console.log("✔ All cancelled orders purged.");
```

_(Note: These methods are also fully supported within the `tx` Transaction object)._

---

## 🔬 Advanced Queries with the `Query` Object

The `Query` interface is the most powerful feature of the client, allowing you to build complex, server-side queries. LunaDB executes these using in-memory B-Trees and Zero-Copy BSON streams, making them incredibly fast.

### Query Parameters

- `filter` (object): Defines conditions to select items (like a `WHERE` clause).

- `order_by` (array): Sorts the results based on one or more fields (`[{"field": "age", "direction": "desc"}]`).

- `limit` (number): Restricts the maximum number of results returned.

- `offset` (number): Skips a specified number of results. Highly optimized for Deep Pagination.

- `count` (boolean): If `true`, returns a raw count of matching items.

- `distinct` (string): Returns a list of unique values for the specified field.

- `group_by` (array): Groups results by one or more fields to perform aggregations.

- `aggregations` (object): Defines math functions to run on groups (e.g., `sum`, `avg`, `min`, `max`, `count`).

- `having` (object): Filters the results _after_ grouping and aggregation (like a `HAVING` clause).

- **`projection` (array of strings)**: Selects strictly which fields to return, reducing network traffic.

- **`lookups` (array of objects)**: Enriches documents by performing server-side Hash Joins with other collections.

### Building Filters

The `filter` object is the core of your query. It can be a single condition or a nested structure using logical operators like `and`, `or`, and `not`.

**Single Condition Structure:** `{ field: "field_name", op: "operator", value: ... }`

|**Operator (op)**|**Description**|**Example value**|
|---|---|---|
|`=`|Equal to|`"some_string"` or `123`|
|`!=`|Not equal to|`"some_string"` or `123`|
|`>`|Greater than|`100`|
|`>=`|Greater than or equal to|`100`|
|`<`|Less than|`50`|
|`<=`|Less than or equal to|`50`|
|`like`|Regex-backed pattern matching (`%` is wildcard)|`"start%"` or `"%middle%"`|
|`in`|Value is in a list of possibilities|`["value1", "value2"]`|
|`between`|Value is between two values (inclusive)|`[10, 20]`|
|`is null`|The field does not exist or is `null`|`true` (or any value)|
|`is not null`|The field exists and is not `null`|`true` (or any value)|

### Example: Complex Filter

This query finds all users who are active and older than 30.

```JavaScript
const query = {
  filter: {
    and: [
      { field: "active", op: "=", value: true },
      { field: "age", op: ">", value: 30 },
    ],
  },
};
const results = await client.collectionQuery("users", query);
```

### Joins and Data Enrichment with `lookups`

The **`lookups`** parameter allows you to perform powerful, zero-copy server-side joins.

```JavaScript
// Enriches 'profiles' with data from 'users'
const lookupQuery = {
  lookups: [
    {
      from: "users",
      localField: "user_id", // Field from the 'profiles' collection
      foreignField: "_id",   // Field from the 'users' collection
      as: "user_info",       // New field to store the joined user document
    },
  ],
};

const enrichedProfiles = await client.collectionQuery("profiles", lookupQuery);
```

### Deep Query Example: Aggregations & Grouping

```JavaScript
const analyticsQuery = {
  // 1. Group documents by the 'active' field
  group_by: ["active"],

  // 2. Define the aggregations to perform on each group
  aggregations: {
    userCount: { func: "count", field: "_id" }, // Count documents
    maxAge: { func: "max", field: "age" },      // Find max age
  },
};

const report = await client.collectionQuery("users", analyticsQuery);
```

---

## ⚡ API Reference

### Connection and Session

- **`constructor(host, port, username?, password?, serverCertPath?, rejectUnauthorized?, poolSize?)`**: Creates a new client instance.

- **`connect(): Promise<void>`**: Establishes the TLS connection pool and authenticates.

- **`close(): void`**: Closes all underlying socket connections.

- **`isAuthenticatedSession`**: Boolean indicating if the client is authenticated.

- **`authenticatedUser`**: String containing the username of the authenticated user.

### Transaction Operations (`Tx` Object)

- **`client.begin(): Promise<Tx>`**: Starts a new transaction and locks a connection.

- **`tx.commit(): Promise<string>`**: Commits the transaction to disk.

- **`tx.rollback(): Promise<string>`**: Discards changes and releases the connection.

- **`tx.collectionItemUpdate(...)`**: (And other mutation methods) Execute operations within the transaction scope.

### Collection & Index Operations

- **`collectionCreate(name: string): Promise<string>`**: Creates a new bucket on disk.

- **`collectionDelete(name: string): Promise<string>`**: Deletes a collection and its indexes.

- **`collectionList(): Promise<string[]>`**: Lists all accessible collections.

- **`collectionIndexCreate(coll: string, field: string): Promise<string>`**: Builds a B-Tree index in RAM.

- **`collectionIndexDelete(coll: string, field: string): Promise<string>`**: Removes an index.

- **`collectionIndexList(coll: string): Promise<string[]>`**: Lists all indexed fields for a collection.

### Item (CRUD) Operations

- **`collectionItemSet<T>(coll: string, value: T, key?: string): Promise<T>`**: Stores an item. Returns the saved BSON document.

- **`collectionItemSetMany<T>(coll: string, items: T[]): Promise<string>`**: Stores multiple items via Group Commit.

- **`collectionItemUpdate<T>(coll: string, key: string, patchValue: Partial<T>): Promise<string>`**: Partially updates a document using BSON patching.

- **`collectionItemUpdateMany<T>(coll: string, items: { _id: string, patch: Partial<T> }[]): Promise<string>`**: Partially updates multiple items in a batch.

- **`collectionItemGet<T>(coll: string, key: string): Promise<GetResult<T>>`**: Retrieves an item.

- **`collectionItemDelete(coll: string, key: string): Promise<string>`**: Deletes a document.

- **`collectionItemDeleteMany(coll: string, keys: string[]): Promise<string>`**: Deletes multiple items by key.

### Mass Mutation Operations

- **`collectionUpdateWhere<T>(coll: string, query: Query, patchValue: Partial<T>): Promise<string>`**: Mass updates all documents matching the query condition using a BSON patch.

- **`collectionDeleteWhere(coll: string, query: Query): Promise<string>`**: Mass deletes all documents matching the query condition.

### Query Operations

- **`collectionQuery<T>(coll: string, query: Query): Promise<T>`**: Executes a complex query returning an array of documents, aggregations, or distinct values.

---

## 🔒 Security Considerations

- **TLS is Mandatory**: LunaDB enforces TLS to encrypt data in transit.

- **Certificate Verification**: For production, always set `rejectUnauthorized` to `true` (the default) and provide a `serverCertPath` to prevent man-in-the-middle attacks.

---

## ❤️ Support the Project

Hello! I'm **Adonay Boscán**, the developer behind **LunaDB**. This is a passionate open-source effort to build a modern, high-performance database engine from scratch.

If this project has helped you scale your applications, consider supporting its continued development. Your contributions allow me to maintain the codebase, implement new features, and keep the project thriving.

### How You Can Help

Every contribution, no matter the size, is a great help and is enormously appreciated.

**[Click here to donate via PayPal](https://paypal.me/AdonayB?locale.x=es_XC&country.x=VE)**

### Other Ways to Contribute

- **Star the project** on GitHub.

- **Share it** with your engineering team.

- **Report bugs** or request features by opening an issue.
