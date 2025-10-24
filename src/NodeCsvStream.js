"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NodeCsvStream = void 0;
var stream_1 = require("stream");
var fastCsv = require("fast-csv");
/**
 * @class NodeCsvStream
 * @description A standalone, framework-agnostic utility class
 * designed for efficiently streaming large datasets
 * from an asynchronous data source
 * directly to a NodeJS Writable Stream (like an HTTP response).
 * All methods are static, making this a pure utility class suitable
 * for various environments (e.g., NestJS, Express, Hapi, or pure Node HTTP).
 */
var NodeCsvStream = /** @class */ (function () {
    function NodeCsvStream() {
    }
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
    NodeCsvStream.createDataReadableStream = function (fetchFn, batchSize, query) {
        var currentOffset = 0;
        var hasMore = true;
        var readable = new stream_1.Readable({
            objectMode: true,
            read: function () {
                return __awaiter(this, void 0, void 0, function () {
                    var data, _i, data_1, row, err_1;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                _a.trys.push([0, 2, , 3]);
                                if (!hasMore) {
                                    this.push(null); // Signal end of stream
                                    return [2 /*return*/];
                                }
                                return [4 /*yield*/, fetchFn(currentOffset, batchSize, query)];
                            case 1:
                                data = _a.sent();
                                if (!Array.isArray(data) || data.length === 0) {
                                    hasMore = false;
                                    this.push(null); // Signal end of stream if no more data is returned
                                    return [2 /*return*/];
                                }
                                for (_i = 0, data_1 = data; _i < data_1.length; _i++) {
                                    row = data_1[_i];
                                    this.push(row);
                                }
                                currentOffset += batchSize;
                                return [3 /*break*/, 3];
                            case 2:
                                err_1 = _a.sent();
                                // Emit error and destroy the stream on fetch failure
                                this.destroy(err_1);
                                return [3 /*break*/, 3];
                            case 3: return [2 /*return*/];
                        }
                    });
                });
            },
        });
        return readable;
    };
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
    NodeCsvStream.createDataStreamTransformer = function (fileMapping, docsFormattingFn) {
        return new stream_1.Transform({
            objectMode: true,
            transform: function (chunk, _, callback) {
                return __awaiter(this, void 0, void 0, function () {
                    var transformed, err_2;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                _a.trys.push([0, 2, , 3]);
                                return [4 /*yield*/, docsFormattingFn(chunk, fileMapping)];
                            case 1:
                                transformed = _a.sent();
                                callback(null, transformed); // Pass transformed object to the next stream
                                return [3 /*break*/, 3];
                            case 2:
                                err_2 = _a.sent();
                                callback(err_2); // Pass error to stream
                                return [3 /*break*/, 3];
                            case 3: return [2 /*return*/];
                        }
                    });
                });
            },
        });
    };
    /**
     * @static
     * @method prepareCsvDocs
     * @description The default utility function used to map source document fields to output CSV headers.
     * It handles basic field mapping and simple concatenation of multiple fields (e.g., "firstName-lastName").
     * @param {any} chunk - A single raw document object fetched from the data source.
     * @param {Record<string, any>} fileMapping - The mapping definition: { "CSV Header": "sourceField" }.
     * @returns {Record<string, any>} An object where keys are the final CSV headers and values are the mapped data.
     */
    NodeCsvStream.prepareCsvDocs = function (chunk, fileMapping) {
        var headers = {};
        for (var _i = 0, _a = Object.entries(fileMapping); _i < _a.length; _i++) {
            var _b = _a[_i], key = _b[0], value = _b[1];
            // ... (Your existing logic for concatenating fields)
            if (typeof value === "string" && value.includes("-")) {
                var fields = value.split("-");
                var rowData = fields
                    .map(function (f) {
                    return chunk && chunk[f] != null ? String(chunk[f]).trim() : "";
                })
                    .filter(Boolean)
                    .join(" ");
                headers[key] = rowData;
            }
            else {
                headers[key] = chunk && chunk[value] != null ? chunk[value] : "";
            }
        }
        return headers;
    };
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
    NodeCsvStream.downloadCsv = function (res_1, fileMapping_1, documentFetcher_1) {
        return __awaiter(this, arguments, void 0, function (res, fileMapping, documentFetcher, docsFormattingFn, batchSize, query) {
            var transformToCsv_1, readable_1, err_3;
            if (docsFormattingFn === void 0) { docsFormattingFn = NodeCsvStream.prepareCsvDocs; }
            if (batchSize === void 0) { batchSize = 1000; }
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        // Input validation check
                        if (!res ||
                            typeof res.write !== "function" ||
                            typeof res.end !== "function") {
                            throw new Error("The downloadCsv function requires a valid NodeJS Writable Stream.");
                        }
                        transformToCsv_1 = NodeCsvStream.createDataStreamTransformer(fileMapping, docsFormattingFn);
                        readable_1 = NodeCsvStream.createDataReadableStream(documentFetcher, batchSize, query);
                        // Wrap the streaming pipeline in a promise to handle finish and error events
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                var stream = readable_1
                                    .pipe(transformToCsv_1)
                                    // fast-csv formats the objects into a stream of CSV text
                                    .pipe(fastCsv.format({ headers: true, quoteColumns: false }))
                                    .pipe(res); // Pipe the final CSV text to the destination stream
                                stream.on("error", function (err) {
                                    reject(err);
                                });
                                stream.on("finish", function () { return resolve(); });
                            })];
                    case 1:
                        // Wrap the streaming pipeline in a promise to handle finish and error events
                        _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_3 = _a.sent();
                        throw err_3;
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    return NodeCsvStream;
}());
exports.NodeCsvStream = NodeCsvStream;
