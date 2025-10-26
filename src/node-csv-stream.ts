import { Readable, Transform, Writable } from "stream";
import * as fastCsv from "fast-csv";
import { DocsFormattingFn, DocumentFetcher } from "./types";

/**
 * @class NodeCsvStream
 * @description A standalone, framework-agnostic utility class
 * designed for efficiently streaming large datasets
 * from an asynchronous data source
 * directly to a NodeJS Writable Stream (like an HTTP response).
 * All methods are static, making this a pure utility class suitable
 * for various environments (e.g., NestJS, Express, Hapi, or pure Node HTTP).
 */
export class NodeCsvStream {
  /**
   * @static
   * @method createDataReadableStream
   * @description Creates a Node.js Readable stream in object mode
   * that pulls data from the source asynchronously in defined batches.
   * The stream stops when the provided `fetchFn` returns an empty array.
   * @param {DocumentFetcher} fetchFn - The asynchronous function responsible for fetching data.
   * It must accept `offset`, `limit`, and the opaque `query`.
   * @param {number} batchSize - The number of records to fetch in each asynchronous call.
   * This controls backpressure and memory usage.
   * @param {any} [query] - An opaque query object passed directly to the `fetchFn`.
   * Used for filtering or scoping data.
   * @returns {Readable} A Node.js Readable stream in object mode emitting raw document objects.
   */
  static createDataReadableStream(
    fetchFn: DocumentFetcher,
    batchSize: number,
    query?: any
  ): Readable {
    let currentOffset = 0;
    let hasMore = true;

    const readable = new Readable({
      objectMode: true,
      async read() {
        try {
          if (!hasMore) {
            this.push(null); // Signal end of stream
            return;
          }

          const data = await fetchFn(currentOffset, batchSize, query);
          if (!Array.isArray(data) || data.length === 0) {
            hasMore = false;
            this.push(null); // Signal end of stream if no more data is returned
            return;
          }

          for (const row of data) {
            this.push(row);
          }

          currentOffset += batchSize;
        } catch (err) {
          // Emit error and destroy the stream on fetch failure
          (this as Readable).destroy(err as Error);
        }
      },
    });

    return readable;
  }

  /**
   * @static
   * @method createDataStreamTransformer
   * @description Creates a Node.js Transform stream responsible for mapping raw data documents (chunks)
   * into the final, formatted row objects ready for CSV serialization.
   * @param {Record<string, any>} fileMapping - The mapping structure defining the output headers
   * and source fields.
   * @param {DocsFormattingFn} docsFormattingFn - The function that performs the transformation logic.
   * It can be synchronous or asynchronous.
   * @returns {Transform} A Node.js Transform stream in object mode emitting formatted row objects.
   */
  static createDataStreamTransformer(
    fileMapping: Record<string, any>,
    docsFormattingFn: DocsFormattingFn
  ): Transform {
    return new Transform({
      objectMode: true,
      async transform(chunk: Record<string, any>, _, callback) {
        try {
          const transformed = await docsFormattingFn(chunk, fileMapping);
          callback(null, transformed); // Pass transformed object to the next stream
        } catch (err) {
          callback(err as Error); // Pass error to stream
        }
      },
    });
  }

  /**
   * @static
   * @method prepareDocs
   * @description The default utility function used to map source document fields to output CSV headers.
   * It handles basic field mapping and simple concatenation of multiple fields (e.g., "firstName-lastName").
   * @param {any} chunk - A single raw document object fetched from the data source.
   * @param {Record<string, any>} fileMapping - The mapping definition: { "CSV Header": "sourceField" }.
   * @returns {Record<string, any>} An object where keys are the final CSV headers and values are the mapped data.
   */
  static prepareDocs(chunk: any, fileMapping: Record<string, any>) {
    const headers: Record<string, any> = {};

    for (const [key, value] of Object.entries(fileMapping)) {
      // ... (Your existing logic for concatenating fields)
      if (typeof value === "string" && value.includes("-")) {
        const fields = value.split("-");
        const rowData = fields
          .map((f) =>
            chunk && chunk[f] != null ? String(chunk[f]).trim() : ""
          )
          .filter(Boolean)
          .join(" ");
        headers[key] = rowData;
      } else {
        headers[key] = chunk && chunk[value] != null ? chunk[value] : "";
      }
    }

    return headers;
  }
  /**
   * @static
   * @async
   * @method downloadCsv
   * @description The main entry point to initiate the CSV download pipeline.
   * It chains the readable stream, the transformer, the CSV formatter,
   * and pipes the result into the destination stream.
   * NOTE: The consumer is responsible for setting all necessary HTTP headers
   * (e.g., Content-Type, Content-Disposition)
   * on the `res` object BEFORE calling this method.
   *
   * @param {Writable} res - The destination Node.js Writable stream
   * (e.g., the Express or native HTTP response object).
   * @param {Record<string, any>} fileMapping - The mapping structure for column headers.
   * @param {DocumentFetcher} documentFetcher - The function used to fetch data in batches.
   * @param {DocsFormattingFn} [docsFormattingFn=prepareCsvDocs] - Optional custom formatter function.
   * @param {number} [batchSize=1000] - The number of records to fetch per database call.
   * @param {any} [query] - Optional query parameters passed to the `documentFetcher`.
   * @returns {Promise<void>} A promise that resolves when the stream piping is complete
   * and the file has been fully sent.
   * @throws {Error} Throws an error if the provided `res` is not a valid Writable Stream
   * or if any error occurs during streaming.
   */
  static async download(
    res: Writable,
    fileMapping: Record<string, any>,
    documentFetcher: DocumentFetcher,
    query?: any,
    docsFormattingFn: DocsFormattingFn = NodeCsvStream.prepareDocs,
    batchSize = 1000
  ): Promise<void> {
    try {
      // Input validation check for output destination
      if (
        !res ||
        typeof res.write !== "function" ||
        typeof res.end !== "function"
      ) {
        throw new Error(
          "The downloadCsv function requires a valid NodeJS Writable Stream."
        );
      }

      const transformToCsv = NodeCsvStream.createDataStreamTransformer(
        fileMapping,
        docsFormattingFn
      );

      const readable = NodeCsvStream.createDataReadableStream(
        documentFetcher,
        batchSize,
        query
      );

      // Wrap the streaming pipeline in a promise to handle finish and error events
      await new Promise<void>((resolve, reject) => {
        const stream = readable
          .pipe(transformToCsv)
          // fast-csv formats the objects into a stream of CSV text
          .pipe(fastCsv.format({ headers: true, quoteColumns: false }))
          .pipe(res); // Pipe the final CSV text to the destination stream

        stream.on("error", (err) => {
          reject(err);
        });

        stream.on("finish", () => resolve());
      });
    } catch (err) {
      throw err;
    }
  }
}
