import type { Dataset, DatasetMetaQuarterly, DatasetMetaYearly } from "./dataset";

export type WageMetric = "gross_eur" | "net_eur";

export type WageGroup =
  | "average"
  | "public_sector"
  | "public_enterprises"
  | "private_sector";

export type WageRecord = {
  period: string;
  group: WageGroup;
  gross_eur: number | null;
  net_eur: number | null;
};

export type EmploymentMetric = "employment";

export type EmploymentGender = "male" | "female" | "total";

export type EmploymentActivityGenderRecord = {
  period: string;
  activity: string;
  gender: EmploymentGender;
  employment: number | null;
};

export type WageLevelsMeta = DatasetMetaYearly<WageMetric, "group">;
export type WageLevelsDataset = Dataset<WageRecord, WageLevelsMeta>;

export type EmploymentActivityGenderMeta = DatasetMetaQuarterly<
  EmploymentMetric,
  "activity" | "gender"
>;

export type EmploymentActivityGenderDataset = Dataset<
  EmploymentActivityGenderRecord,
  EmploymentActivityGenderMeta
>;
