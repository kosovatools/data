import {
  PATHS,
  type EducationBachelorFirstTimeRecord,
  type EducationGender,
} from "@kosovatools/data";
import { runPxDatasetPipeline } from "../pipeline/px-dataset";
import { normalizeWhitespace, slugifyLabel } from "../lib/utils";

const STUDENT_METRIC = {
  code: "__value__",
  key: "students",
  label: "Students",
  unit: "students",
} as const;

const GENDER_MAP: Record<string, { key: EducationGender; label: string }> = {
  "0": { key: "female", label: "Femra" },
  "1": { key: "male", label: "Meshkuj" },
  "2": { key: "total", label: "Gjithsej" },
};

function normalizeAcademicYear(label: string): string {
  const cleaned = normalizeWhitespace(label);
  const match = cleaned.match(/(\d{4})\s*[/\-]?\s*(\d{2,4})?/);
  if (!match) return cleaned || label;
  const start = match[1] ?? "";
  const endRaw = match[2];
  const end =
    endRaw && endRaw.length === 2
      ? `${start.slice(0, 2)}${endRaw}`
      : endRaw ?? "";
  return end ? `${start}/${end}` : start;
}

function academicYearSortValue(label: string): number {
  const normalized = normalizeAcademicYear(label);
  const match = normalized.match(/(\d{4})/);
  if (match) return Number(match[1]);
  return Number.MAX_SAFE_INTEGER;
}

export async function fetchEducationBachelorFirstTime(
  outDir: string,
  generatedAt: string,
) {
  const datasetId = "kas_education_bachelor_first_time_field_gender_yearly";
  return runPxDatasetPipeline<EducationBachelorFirstTimeRecord>({
    datasetId,
    filename: "kas_education_bachelor_first_time_field_gender_yearly.json",
    parts: PATHS.education_bachelor_first_time,
    outDir,
    generatedAt,
    timeDimension: {
      code: "viti",
      text: "viti",
      alias: "period",
      toLabel: (_code, ctx) =>
        normalizeAcademicYear(ctx.value.metaLabel || ctx.value.label || _code),
      sort: (a, b) => academicYearSortValue(a.label) - academicYearSortValue(b.label),
      granularity: "yearly",
    },
    axes: [
      {
        code: "fusha e studimit",
        text: "fusha e studimit",
        alias: "field",
        resolveValues: ({ baseValues }) =>
          baseValues
            .map((entry) => {
              const label = normalizeWhitespace(
                entry.metaLabel || entry.label || entry.code,
              );
              const key = slugifyLabel(label);
              const isTotal =
                key === "gjithsej" || label.toLowerCase().startsWith("gjithsej");
              if (isTotal) return null;
              return { code: entry.code, key, label };
            })
            .filter(
              (
                entry,
              ): entry is { code: string; key: string; label: string } =>
                Boolean(entry),
            ),
      },
      {
        code: "gjinia",
        text: "gjinia",
        alias: "gender",
        resolveValues: ({ baseValues }) =>
          baseValues.map((entry) => {
            const mapping = GENDER_MAP[entry.code];
            const label = normalizeWhitespace(
              entry.metaLabel || entry.label || entry.code,
            );
            return {
              code: entry.code,
              key:
                mapping?.key ??
                ((slugifyLabel(label) as EducationGender) || "total"),
              label: mapping?.label ?? label,
            };
          }),
      },
    ],
    metricDimensions: [
      {
        code: () => null,
        values: [STUDENT_METRIC],
      },
    ],
    createRecord: ({ period, axes, values }) => {
      const fieldAxis = axes.field;
      const genderAxis = axes.gender;
      if (!fieldAxis || !genderAxis) return null;
      return {
        period,
        field: fieldAxis.value.key || fieldAxis.code,
        gender:
          (genderAxis.value.key as EducationGender | undefined) ?? "total",
        students: values.students ?? null,
      };
    },
    buildNotes: () => [
      "Periods represent academic years (p.sh. 2024/2025).",
    ],
    finalizeDataset: ({ records, meta }) => {
      const sortedRecords = [...records].sort((a, b) => {
        if (a.period === b.period) {
          if (a.field === b.field)
            return a.gender.localeCompare(b.gender);
          return a.field.localeCompare(b.field);
        }
        return academicYearSortValue(a.period) - academicYearSortValue(b.period);
      });
      const periods = Array.from(new Set(sortedRecords.map((r) => r.period)));
      const orderedPeriods = periods.sort(
        (a, b) => academicYearSortValue(a) - academicYearSortValue(b),
      );

      return {
        meta: {
          ...meta,
          time: {
            ...meta.time,
            first: orderedPeriods[0] ?? meta.time.first,
            last: orderedPeriods[orderedPeriods.length - 1] ?? meta.time.last,
            count: orderedPeriods.length || meta.time.count,
          },
        },
        records: sortedRecords,
      };
    },
  });
}
