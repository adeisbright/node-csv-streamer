import { createServer } from "http";
import { NodeCsvStream } from "../src/NodeCsvStream";

const numbers = [
  {
    num: 1,
  },
  {
    num: 2,
  },
  {
    num: 3,
  },
  {
    num: 4,
  },
];

const server = createServer(async (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/csv; charset-utf-8",
    "Content-Disposition": `attachment;fileName=s.csv`,
  });
  const documentFetcher = async (
    offset: number,
    limit: number
  ): Promise<any[]> => {
    // In a real app, this would be an async DB call (e.g., Sequelize findAll)

    // Simulate fetching a slice of the 'numbers' array
    return numbers.slice(offset, offset + limit);
  };
  await NodeCsvStream.downloadCsv(res, { num: "num" }, documentFetcher);
});

server.listen(3200, () => console.log("Server Started on Port 3200"));
