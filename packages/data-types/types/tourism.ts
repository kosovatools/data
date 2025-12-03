import type { Dataset, DatasetMetaMonthly } from "./dataset";

export type TourismMetric = "visitors" | "nights";

export type TourismRegionRecord = {
  period: string;
  region: string;
  visitor_group: "total" | "local" | "external";
  visitors: number | null;
  nights: number | null;
};
export type TourismCountryRecord = {
  period: string;
  country: string;
  visitors: number | null;
  nights: number | null;
};

export type TourismRegionMeta = DatasetMetaMonthly<
  TourismMetric,
  "region" | "visitor_group"
>;
export type TourismRegionDataset = Dataset<
  TourismRegionRecord,
  TourismRegionMeta
>;

export type TourismCountryMeta = DatasetMetaMonthly<
  TourismMetric,
  "country"
>;
export type TourismCountryDataset = Dataset<
  TourismCountryRecord,
  TourismCountryMeta
>;
