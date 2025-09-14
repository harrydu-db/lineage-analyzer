export interface Table {
  name: string;
  owner: string;
  isVolatile: boolean;
  isView: boolean;
  sources: TableRelationship[];
  targets: TableRelationship[];
  script?: string;
}

export interface TableRelationship {
  table: string;
  operations: string[];
  statements: Statement[];
}

export interface Statement {
  id: string;
  sql: string;
  file: string;
  line: number;
}

export interface LineageData {
  tables: { [key: string]: Table };
  statements: { [key: string]: Statement };
  scripts?: { [key: string]: any }; // Add scripts data for SQL statements
  summary: {
    totalTables: number;
    sourceTables: number;
    targetTables: number;
    volatileTables: number;
    totalOperations: number;
    scriptCount: number;
    scriptDisplay: string;
  };
}

export interface NetworkNode {
  id: string;
  label: string;
  group: string;
  title?: string;
  color?: string;
  size?: number;
  font?: {
    size: number;
    face: string;
  };
  x?: number;
  y?: number;
}

export interface NetworkEdge {
  id: string;
  from: string;
  to: string;
  label?: string;
  arrows?: string;
  color?: {
    color: string;
    opacity?: number;
  };
  width?: number;
  title?: string;
  font?: {
    size: number;
    color: string;
  };
}

export interface NetworkData {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
}
