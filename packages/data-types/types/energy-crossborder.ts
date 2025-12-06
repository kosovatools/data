import type {
  Dataset,
  DatasetMeta,
  DatasetMetaBaseExtras,
  DatasetMetaMonthly,
} from "./dataset";

export type EnergyMonthlyMetric = "import" | "export" | "net";

export type EnergyMonthlyRecord = {
  period: string;
  neighbor: string;
  import: number;
  export: number;
  net: number;
  has_data: boolean;
};

export type EnergyMonthlyDatasetMeta = DatasetMetaMonthly<
  EnergyMonthlyMetric | "has_data",
  "neighbor"
>;

export type EnergyDailyRecord = {
  period: string;
  neighbor: string;
  import: number;
  export: number;
  net: number;
};

export type EnergyDailyDatasetMeta = DatasetMeta<
  EnergyMonthlyMetric,
  "neighbor",
  "daily",
  DatasetMetaBaseExtras
>;

export type EnergyFlowTotals = {
  importMWh: number;
  exportMWh: number;
  netMWh: number;
};

export type EnergyFlowResult = {
  code: string;
  country: string;
  importMWh: number;
  exportMWh: number;
  netMWh: number;
  hasData: boolean;
};

export type EnergyMonthlyDataset = Dataset<
  EnergyMonthlyRecord,
  EnergyMonthlyDatasetMeta
>;

export type EnergyDailyDataset = Dataset<
  EnergyDailyRecord,
  EnergyDailyDatasetMeta
>;
