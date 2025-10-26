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
var stream_1 = require("stream");
var node_csv_stream_1 = require("../src/node-csv-stream");
describe("NodeCsvStream", function () {
    beforeEach(function () {
        jest.clearAllMocks();
    });
    // Sample data for mock fetcher
    var MOCK_DATA = [
        {
            id: 1,
            firstName: "A",
            lastName: "Ason",
            email: "a@test.com",
            isActive: true,
        },
        {
            id: 2,
            firstName: "B",
            lastName: "Bson",
            email: "b@test.com",
            isActive: false,
        },
        {
            id: 3,
            firstName: "C",
            lastName: "Cson",
            email: "c@test.com",
            isActive: true,
        },
        {
            id: 4,
            firstName: "D",
            lastName: "Dson",
            email: "d@test.com",
            isActive: false,
        },
    ];
    var MOCK_MAPPING = {
        "Full Name": "firstName-lastName",
        "Email Address": "email",
        Status: "isActive",
    };
    describe("prepare", function () {
        it("should map single fields correctly", function () {
            var chunk = MOCK_DATA[0];
            var result = node_csv_stream_1.NodeCsvStream.prepareDocs(chunk, {
                ID: "id",
                Mail: "email",
            });
            expect(result).toEqual({ ID: 1, Mail: "a@test.com" });
        });
        it("should concatenate hyphen-separated fields correctly", function () {
            var chunk = MOCK_DATA[1];
            var result = node_csv_stream_1.NodeCsvStream.prepareDocs(chunk, MOCK_MAPPING);
            expect(result["Full Name"]).toBe("B Bson");
        });
        it("should handle null/undefined source fields by returning an empty string", function () {
            var chunk = { id: 5, firstName: "E", lastName: null, email: undefined };
            var result = node_csv_stream_1.NodeCsvStream.prepareDocs(chunk, {
                Name: "firstName-lastName",
                Email: "email",
                Missing: "nonExistent",
            });
            expect(result["Name"]).toBe("E");
            expect(result["Email"]).toBe("");
            expect(result["Missing"]).toBe("");
        });
    });
    describe("createDataStreamTransformer", function () {
        it("should correctly transform data using a synchronous formatter", function (done) {
            var syncFormatter = function (chunk, mapper) { return ({
                ID: chunk.id,
                Name: "".concat(chunk.firstName, " ").concat(chunk.lastName),
            }); };
            var transformer = node_csv_stream_1.NodeCsvStream.createDataStreamTransformer({}, syncFormatter);
            var testChunk = MOCK_DATA[0];
            transformer.on("data", function (data) {
                expect(data).toEqual({ ID: 1, Name: "A Ason" });
                done();
            });
            transformer.write(testChunk);
            transformer.end();
        });
        it("should correctly transform data using an asynchronous formatter", function (done) {
            var asyncFormatter = function (chunk, mapper) { return __awaiter(void 0, void 0, void 0, function () {
                return __generator(this, function (_a) {
                    switch (_a.label) {
                        case 0: return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, 5); })];
                        case 1:
                            _a.sent(); // Simulate async delay
                            return [2 /*return*/, {
                                    AsyncID: chunk.id * 10,
                                }];
                    }
                });
            }); };
            var transformer = node_csv_stream_1.NodeCsvStream.createDataStreamTransformer({}, asyncFormatter);
            var testChunk = MOCK_DATA[2];
            transformer.on("data", function (data) {
                expect(data).toEqual({ AsyncID: 30 });
                done();
            });
            transformer.write(testChunk);
            transformer.end();
        });
        it("should emit an error if the formatter fails", function (done) {
            var error = new Error("Formatting failed");
            var failingFormatter = function () {
                throw error;
            };
            var transformer = node_csv_stream_1.NodeCsvStream.createDataStreamTransformer({}, failingFormatter);
            transformer.on("error", function (err) {
                expect(err).toBe(error);
                done();
            });
            transformer.write(MOCK_DATA[0]);
        });
    });
    describe("createDataReadableStream", function () {
        // Mock the DocumentFetcher to return data in batches
        var mockFetcher = jest.fn(function (offset, limit, query) { return __awaiter(void 0, void 0, void 0, function () {
            var data;
            return __generator(this, function (_a) {
                if (query === "error") {
                    throw new Error("Database error");
                }
                data = MOCK_DATA.slice(offset, offset + limit);
                return [2 /*return*/, data.length > 0 ? data : []];
            });
        }); });
        it("should fetch data in correct batches and push all items", function (done) {
            var batchSize = 2;
            var readable = node_csv_stream_1.NodeCsvStream.createDataReadableStream(mockFetcher, batchSize);
            var receivedData = [];
            readable.on("data", function (chunk) { return receivedData.push(chunk); });
            readable.on("end", function () {
                expect(mockFetcher).toHaveBeenCalledTimes(3); // 2 batches of 2, 1 final check for empty
                expect(mockFetcher).toHaveBeenCalledWith(0, 2, undefined);
                expect(mockFetcher).toHaveBeenCalledWith(2, 2, undefined);
                expect(mockFetcher).toHaveBeenCalledWith(4, 2, undefined);
                expect(receivedData).toHaveLength(4);
                expect(receivedData).toEqual(MOCK_DATA);
                done();
            });
        });
        it.skip("should destroy the stream on fetchFn error", function (done) {
            var readable = node_csv_stream_1.NodeCsvStream.createDataReadableStream(mockFetcher, 1, "error");
            readable.on("error", function (err) {
                try {
                    expect(err).toBeInstanceOf(Error);
                    expect(err.message).toBe("Database error");
                    done(); // ✅ mark test as passed
                }
                catch (e) {
                    done(e); // fail test if assertion fails
                }
            });
            // Since read is called implicitly, we just need to wait for the error
        });
        it.skip("should stop reading when fetchFn returns an empty array", function (done) {
            // Fetcher is already set up to return empty array when offset > 3
            var batchSize = 10;
            var readable = node_csv_stream_1.NodeCsvStream.createDataReadableStream(mockFetcher, batchSize);
            var receivedData = [];
            readable.on("data", function (chunk) { return receivedData.push(chunk); });
            readable.on("end", function () {
                expect(mockFetcher).toHaveBeenCalledWith(0, 10, undefined);
                expect(mockFetcher).toHaveBeenCalledWith(4, 10, undefined);
                expect(receivedData).toHaveLength(4);
                done();
            });
        });
    });
    describe("download", function () {
        var mockWritable;
        var mockDocumentFetcher;
        beforeEach(function () {
            mockWritable = new stream_1.PassThrough(); // PassThrough is a mock Writable Stream
            mockDocumentFetcher = jest.fn(function (offset, limit) { return __awaiter(void 0, void 0, void 0, function () { return __generator(this, function (_a) {
                return [2 /*return*/, MOCK_DATA.slice(offset, offset + limit)];
            }); }); });
        });
        it("should successfully pipe data through the entire stream chain", function () { return __awaiter(void 0, void 0, void 0, function () {
            var receivedOutput, outputString;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        receivedOutput = [];
                        mockWritable.on("data", function (chunk) {
                            receivedOutput.push(chunk.toString());
                        });
                        // Act
                        return [4 /*yield*/, node_csv_stream_1.NodeCsvStream.download(mockWritable, MOCK_MAPPING, mockDocumentFetcher)];
                    case 1:
                        // Act
                        _a.sent();
                        expect(receivedOutput.length).toBeGreaterThan(0);
                        outputString = receivedOutput.join("");
                        expect(outputString).toContain("A Ason");
                        expect(outputString).toContain("d@test.com");
                        return [2 /*return*/];
                }
            });
        }); });
        it("should throw an error if the writable stream is invalid", function () { return __awaiter(void 0, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: 
                    // Act & Assert
                    return [4 /*yield*/, expect(node_csv_stream_1.NodeCsvStream.download(null, // Pass null to test validation
                        MOCK_MAPPING, mockDocumentFetcher)).rejects.toThrow("The downloadCsv function requires a valid NodeJS Writable Stream.")];
                    case 1:
                        // Act & Assert
                        _a.sent();
                        return [4 /*yield*/, expect(node_csv_stream_1.NodeCsvStream.download({ write: "not a function" }, // Invalid object
                            MOCK_MAPPING, mockDocumentFetcher)).rejects.toThrow("The downloadCsv function requires a valid NodeJS Writable Stream.")];
                    case 2:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        }); });
        it.skip("should reject the promise if an error occurs in the readable stream", function () { return __awaiter(void 0, void 0, void 0, function () {
            var failingFetcher;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        failingFetcher = jest.fn(function () { return __awaiter(void 0, void 0, void 0, function () {
                            return __generator(this, function (_a) {
                                throw new Error("Readable Source Failed");
                            });
                        }); });
                        return [4 /*yield*/, expect(node_csv_stream_1.NodeCsvStream.download(mockWritable, MOCK_MAPPING, failingFetcher)).rejects.toThrow("Readable Source Failed")];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        }); });
        it("should reject the promise if an error occurs in the writable stream", function () { return __awaiter(void 0, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        // Force an error on the writable stream during piping
                        mockWritable.write = jest.fn(function () {
                            mockWritable.emit("error", new Error("Writable Sink Failed"));
                            return true;
                        });
                        return [4 /*yield*/, expect(node_csv_stream_1.NodeCsvStream.download(mockWritable, MOCK_MAPPING, mockDocumentFetcher)).rejects.toThrow("Writable Sink Failed")];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        }); });
    });
});
