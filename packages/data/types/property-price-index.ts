import type { Dataset, DatasetMetaQuarterly } from "./dataset";

export type PropertyPriceIndexMetric = "index";

export type PropertyPriceIndexRecord = {
  period: string;
  index: number | null;
};

export type PropertyPriceIndexMeta =
  DatasetMetaQuarterly<PropertyPriceIndexMetric>;

export type PropertyPriceIndexDataset = Dataset<
  PropertyPriceIndexRecord,
  PropertyPriceIndexMeta
>;
