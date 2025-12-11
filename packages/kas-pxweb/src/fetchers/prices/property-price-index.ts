import {
  PATHS,
  type PropertyPriceIndexRecord,
} from "@kosovatools/data";
import { runPxDatasetPipeline } from "../../pipeline/px-dataset";
import { normalizeWhitespace, type MetaField } from "../../lib/utils";

const DATASET_ID = "kas_property_price_index_quarterly";
const FILENAME = "kas_property_price_index_quarterly.json";

const METRIC_FIELDS: ReadonlyArray<MetaField & { code: string }> = [
  {
    code: "0",
    key: "index",
    label: "Indeksi (2018=100)",
    unit: "indeks (2018=100)",
  },
];

function normalizePeriod(label: string): string {
  const cleaned = normalizeWhitespace(label);
  const yearlyAverage = cleaned.match(/mesatarja\s+vjetore\s+(\d{4})/i);
  if (yearlyAverage && yearlyAverage[1]) {
    return `${yearlyAverage[1]}-A`;
  }

  const quarterMatch = cleaned.match(/(\d{4})\s*(?:tm|q)\s*([1-4])/i);
  if (quarterMatch) {
    const [, year, quarter] = quarterMatch;
    return `${year}-Q${quarter}`;
  }

  const fallbackYear = cleaned.match(/(\d{4})/);
  if (fallbackYear && fallbackYear[1]) {
    return `${fallbackYear[1]}-A`;
  }

  return cleaned || "0000-Q1";
}

export async function fetchPropertyPriceIndex(
  outDir: string,
  generatedAt: string,
) {
  return runPxDatasetPipeline<PropertyPriceIndexRecord>({
    datasetId: DATASET_ID,
    filename: FILENAME,
    parts: PATHS.property_price_index,
    outDir,
    generatedAt,
    timeDimension: {
      code: "Year/quarter",
      text: "Viti/tremujori",
      granularity: "quarterly",
      resolveValues: ({ baseValues }) =>
        baseValues.filter((value) => {
          const label = value.metaLabel || value.label || "";
          return !/mesatarja\s+vjetore/i.test(label);
        }),
      toLabel: (_code, ctx) =>
        normalizePeriod(ctx.value.metaLabel || ctx.value.label || _code),
    },
    metricDimensions: [
      {
        code: "Variables",
        text: "Variabla",
        values: METRIC_FIELDS.map((field) => ({
          code: field.code,
          key: field.key,
          label: field.label,
          unit: field.unit,
        })),
      },
    ],
    createRecord: ({ period, values }) => ({
      period,
      index: values.index ?? null,
    }),
    buildNotes: () => [
      "Vlerat me sufiks '-A' janë 'Mesatarja vjetore' të publikuara nga ASKdata.",
    ],
  });
}
