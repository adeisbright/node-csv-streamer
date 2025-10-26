# node-csv-streamer

**Stream large datasets efficiently and export to CSV — framework-agnostic and NestJS-friendly.**

## 🚀 Features

- ✅ Stream data in chunks (pagination-friendly)
- 🧠 Memory-efficient (uses Node.js streams)
- 🔄 Supports sync or async data transformation
- 🧩 Flexible field mapping (supports concatenation like `firstName-lastName`)
- 🛠️ Works seamlessly with NestJS, Express, or pure Node.js

## Install

```bash
npm install node-csv-streamer
```

## 🧠 Concept

node-csv-streamer is designed for large dataset exports where loading everything into memory isn’t feasible.

Instead of fetching all data at once, it streams your data in batches using a fetchFn(skip, limit, query) function.

This approach ensures low memory usage and smooth CSV generation — ideal for analytics dashboards, data exports, or reporting systems.

## 🧩 Fetch Function Pattern

Your data-fetching method must follow this pattern:

```
async function fetchFn(skip: number, limit: number, query: any): Promise<any[]> {
  // Fetch data from your source
  return await dataSource.find(query).skip(skip).limit(limit); //This is just an example
}
```

skip → starting point for batch retrieval

limit → batch size (default is 1000)

query → optional filtering parameters

Each batch is streamed and written directly to the CSV file.

# Usage

When using node-csv-streamer, the assumption is you are working with a large data set or you want
a memory efficient way to get and send data as csv to a writable stream.

To achieve this, your data fetching method must be setup to get data in batches.
You can set the batch use the existing 1000.

Your data fetching method should be define in this pattern:
fetchFn(skip,limit,query)

The skip is currently used to define how where to start from in fetching data,
the limit is the batchSize(1000) to retrieve at each call to your source data location and
query is the criteria for selecting the data.

This fetching pattern is used over using a method to gets all the data you need into memory.

### Example : Express Controller

```ts
import { NodeCsvStream } from "node-csv-streamer";
import express from "express";

const app = express();

app.get("/download-csv", async (req, res) => {
  // Set response headers for CSV download
  res.setHeader("Content-Type", "text/csv");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="example-csv-file.csv"`
  );

  // Define mapping between CSV headers and your data source fields
  // You can combine multiple fields using a hyphen ("-")
  const csvHeaderMapping = {
    Email: "email",
    Name: "firstName-lastName",
    "Phone Number": "phoneNumber",
  };

  // Stream CSV data directly to the HTTP response
  await NodeCsvStream.download(
    res,
    csvHeaderMapping,
    aggregateEmployeeRecords, // or this.aggregateEmployeeRecords.bind(this)
    {}, // optional query parameters
    undefined, // optional formatting function
    100 // batch size
  );
});
```

## ⚙️ API Reference

### `NodeCsvStream.download(res, fileMapping, fetchFn, query?, docsFormattingFn?, batchSize?)`

| Parameter                         | Type                                                          | Description                                                                                                                               |
| --------------------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **res**                           | `Writable`                                                    | Writable stream (e.g., Express or NestJS `Response` object). Typically the HTTP response where the CSV will be streamed directly.         |
| **fileMapping**                   | `Record<string, string>`                                      | Defines the mapping between CSV column headers and data source keys. Supports concatenation with a hyphen (e.g., `"firstName-lastName"`). |
| **fetchFn**                       | `(skip: number, limit: number, query: any) => Promise<any[]>` | Function responsible for fetching data in batches. It is called repeatedly until no more records are returned.                            |
| **query** _(optional)_            | `any`                                                         | Query object passed to `fetchFn` for data filtering or scoping.                                                                           |
| **docsFormattingFn** _(optional)_ | `(doc: any, mapping: Record<string, string>) => any`          | Optional transformation function to modify each record before converting to CSV.                                                          |
| **batchSize** _(optional)_        | `number`                                                      | Number of records to fetch per batch. Defaults to **1000**.                                                                               |

---

### 🧩 Example Usage

```ts
await NodeCsvStream.download(
  res,
  {
    Email: "email",
    Name: "firstName-lastName",
    "Phone Number": "phoneNumber",
  },
  aggregateEmployeeRecords, // async function (skip, limit, query)
  { active: true }, // optional query
  undefined, // optional transform function
  500 // optional batch size
);
```

## 🪶 License

MIT © 2025 — Maintained by [Adeleke Bright](https://githbu.com/adeisbright)
