import type { Dataset, DatasetMetaMonthly } from "./dataset";

export type TradeMetric = "imports" | "exports";

export type TradeChapterRecord = {
  period: string;
  chapter: string;
  imports: number | null;
  exports: number | null;
};

export type TradePartnerRecord = {
  period: string;
  partner: string;
  imports: number | null;
  exports: number | null;
};

export type TradeChaptersMeta = DatasetMetaMonthly<
  TradeMetric,
  "chapter",
  { chaptersLabel?: Record<string, string> }
>;
export type TradeChaptersDataset = Dataset<
  TradeChapterRecord,
  TradeChaptersMeta
>;

export type TradePartnersMeta = DatasetMetaMonthly<
  TradeMetric,
  "partner",
  { partner_labels?: Record<string, string> }
>;
export type TradePartnersDataset = Dataset<
  TradePartnerRecord,
  TradePartnersMeta
>;
