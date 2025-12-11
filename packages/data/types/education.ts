import type { Dataset, DatasetMetaYearly } from "./dataset";

export type EducationGender = "female" | "male" | "total";

export type EducationBachelorFirstTimeRecord = {
  period: string;
  field: string;
  gender: EducationGender;
  students: number | null;
};

export type EducationBachelorFirstTimeMeta = DatasetMetaYearly<
  "students",
  "field" | "gender"
>;

export type EducationBachelorFirstTimeDataset = Dataset<
  EducationBachelorFirstTimeRecord,
  EducationBachelorFirstTimeMeta
>;
