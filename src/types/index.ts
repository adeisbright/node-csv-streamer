// Type for the function that fetches data in batches
export type DocumentFetcher = (
  offset: number,
  limit: number,
  query?: any
) => Promise<any[]>;

// Type for the function that formats raw data chunks
export type DocsFormattingFn = (
  chunk: any,
  mapper: Record<string, any>
) => Promise<any> | any;
