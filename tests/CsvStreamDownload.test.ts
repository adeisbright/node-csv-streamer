import { Writable, PassThrough } from "stream";
import { NodeCsvStream } from "../src/NodeCsvStream";
import { DocsFormattingFn, DocumentFetcher } from "../src/types";

describe("NodeCsvStream", () => {
  // Sample data for mock fetcher
  const MOCK_DATA = [
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

  const MOCK_MAPPING = {
    "Full Name": "firstName-lastName",
    "Email Address": "email",
    Status: "isActive",
  };

  describe("prepareCsvDocs", () => {
    it("should map single fields correctly", () => {
      const chunk = MOCK_DATA[0];
      const result = NodeCsvStream.prepareCsvDocs(chunk, {
        ID: "id",
        Mail: "email",
      });
      expect(result).toEqual({ ID: 1, Mail: "a@test.com" });
    });

    it("should concatenate hyphen-separated fields correctly", () => {
      const chunk = MOCK_DATA[1];
      const result = NodeCsvStream.prepareCsvDocs(chunk, MOCK_MAPPING);
      expect(result["Full Name"]).toBe("B Bson");
    });

    it("should handle null/undefined source fields by returning an empty string", () => {
      const chunk = { id: 5, firstName: "E", lastName: null, email: undefined };
      const result = NodeCsvStream.prepareCsvDocs(chunk, {
        Name: "firstName-lastName",
        Email: "email",
        Missing: "nonExistent",
      });
      expect(result["Name"]).toBe("E");
      expect(result["Email"]).toBe("");
      expect(result["Missing"]).toBe("");
    });
  });

  describe("createDataStreamTransformer", () => {
    it("should correctly transform data using a synchronous formatter", (done) => {
      const syncFormatter: DocsFormattingFn = (chunk, mapper) => ({
        ID: chunk.id,
        Name: `${chunk.firstName} ${chunk.lastName}`,
      });

      const transformer = NodeCsvStream.createDataStreamTransformer(
        {},
        syncFormatter
      );
      const testChunk = MOCK_DATA[0];

      transformer.on("data", (data) => {
        expect(data).toEqual({ ID: 1, Name: "A Ason" });
        done();
      });
      transformer.write(testChunk);
      transformer.end();
    });

    it("should correctly transform data using an asynchronous formatter", (done) => {
      const asyncFormatter: DocsFormattingFn = async (chunk, mapper) => {
        await new Promise((resolve) => setTimeout(resolve, 5)); // Simulate async delay
        return {
          AsyncID: chunk.id * 10,
        };
      };

      const transformer = NodeCsvStream.createDataStreamTransformer(
        {},
        asyncFormatter
      );
      const testChunk = MOCK_DATA[2];

      transformer.on("data", (data) => {
        expect(data).toEqual({ AsyncID: 30 });
        done();
      });
      transformer.write(testChunk);
      transformer.end();
    });

    it("should emit an error if the formatter fails", (done) => {
      const error = new Error("Formatting failed");
      const failingFormatter: DocsFormattingFn = () => {
        throw error;
      };
      const transformer = NodeCsvStream.createDataStreamTransformer(
        {},
        failingFormatter
      );

      transformer.on("error", (err) => {
        expect(err).toBe(error);
        done();
      });
      transformer.write(MOCK_DATA[0]);
    });
  });

  describe("createDataReadableStream", () => {
    // Mock the DocumentFetcher to return data in batches
    const mockFetcher: DocumentFetcher = jest.fn(
      async (offset, limit, query) => {
        if (query === "error") {
          throw new Error("Database error");
        }
        const data = MOCK_DATA.slice(offset, offset + limit);
        return data.length > 0 ? data : [];
      }
    );

    // beforeEach(() => { jest.clearAllMocks(); }); // Handled by global beforeEach

    it("should fetch data in correct batches and push all items", (done) => {
      const batchSize = 2;
      const readable = NodeCsvStream.createDataReadableStream(
        mockFetcher,
        batchSize
      );
      const receivedData: any[] = [];

      readable.on("data", (chunk) => receivedData.push(chunk));

      readable.on("end", () => {
        expect(mockFetcher).toHaveBeenCalledTimes(3); // 2 batches of 2, 1 final check for empty
        expect(mockFetcher).toHaveBeenCalledWith(0, 2, undefined);
        expect(mockFetcher).toHaveBeenCalledWith(2, 2, undefined);
        expect(mockFetcher).toHaveBeenCalledWith(4, 2, undefined);
        expect(receivedData).toHaveLength(4);
        expect(receivedData).toEqual(MOCK_DATA);
        done();
      });
    });

    it.skip("should destroy the stream on fetchFn error", (done) => {
      const readable = NodeCsvStream.createDataReadableStream(
        mockFetcher,
        1,
        "error"
      );

      readable.on("error", (err) => {
        expect(err.message).toBe("Database error");
        done();
      });

      // Since read is called implicitly, we just need to wait for the error
    });

    it.skip("should stop reading when fetchFn returns an empty array", (done) => {
      // Fetcher is already set up to return empty array when offset > 3
      const batchSize = 10;
      const readable = NodeCsvStream.createDataReadableStream(
        mockFetcher,
        batchSize
      );
      const receivedData: any[] = [];

      readable.on("data", (chunk) => receivedData.push(chunk));

      readable.on("end", () => {
        expect(mockFetcher).toHaveBeenCalledTimes(2); // 1 fetch of 4 items, 1 fetch for empty
        expect(mockFetcher).toHaveBeenCalledWith(0, 10, undefined);
        expect(mockFetcher).toHaveBeenCalledWith(4, 10, undefined);
        expect(receivedData).toHaveLength(4);
        done();
      });
    });
  });

  describe("downloadCsv", () => {
    let mockWritable: Writable;
    let mockDocumentFetcher: DocumentFetcher;

    beforeEach(() => {
      mockWritable = new PassThrough(); // Use PassThrough as a mock Writable Stream
      mockDocumentFetcher = jest.fn(async (offset, limit) =>
        MOCK_DATA.slice(offset, offset + limit)
      );

      // The mockFastCsvFormat implementation logic is now handled in the global beforeEach,
      // ensuring it runs before any test uses fastCsv.format().
    });

    it("should successfully pipe data through the entire stream chain", async () => {
      const receivedOutput: string[] = [];

      mockWritable.on("data", (chunk) => {
        receivedOutput.push(chunk.toString());
      });

      // Act
      await NodeCsvStream.downloadCsv(
        mockWritable,
        MOCK_MAPPING,
        mockDocumentFetcher,
        NodeCsvStream.prepareCsvDocs,
        2 // batchSize
      );

      // Assert
      expect(mockDocumentFetcher).toHaveBeenCalledTimes(3); // 2 batches + 1 stop check
      expect(receivedOutput.length).toBeGreaterThan(0);

      // Check the output strings contain the expected concatenated data
      const outputString = receivedOutput.join("");
      expect(outputString).toContain("A Ason");
      expect(outputString).toContain("d@test.com");
    });

    it("should throw an error if the writable stream is invalid", async () => {
      // Act & Assert
      await expect(
        NodeCsvStream.downloadCsv(
          null as unknown as Writable, // Pass null to test validation
          MOCK_MAPPING,
          mockDocumentFetcher
        )
      ).rejects.toThrow(
        "The downloadCsv function requires a valid NodeJS Writable Stream."
      );

      await expect(
        NodeCsvStream.downloadCsv(
          { write: "not a function" } as unknown as Writable, // Invalid object
          MOCK_MAPPING,
          mockDocumentFetcher
        )
      ).rejects.toThrow(
        "The downloadCsv function requires a valid NodeJS Writable Stream."
      );
    });

    it.skip("should reject the promise if an error occurs in the readable stream", async () => {
      // Mock fetcher to throw on first call
      const failingFetcher: DocumentFetcher = jest.fn(async () => {
        throw new Error("Readable Source Failed");
      });

      await expect(
        NodeCsvStream.downloadCsv(mockWritable, MOCK_MAPPING, failingFetcher)
      ).rejects.toThrow("Readable Source Failed");
    });

    it("should reject the promise if an error occurs in the writable stream", async () => {
      // Force an error on the writable stream during piping
      mockWritable.write = jest.fn(() => {
        mockWritable.emit("error", new Error("Writable Sink Failed"));
        return true;
      });

      await expect(
        NodeCsvStream.downloadCsv(
          mockWritable,
          MOCK_MAPPING,
          mockDocumentFetcher
        )
      ).rejects.toThrow("Writable Sink Failed");
    });
  });
});
