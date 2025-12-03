import type { Dataset, DatasetMetaMonthly, DatasetMetaYearly } from "./dataset";

export type TransportMetric =
  | "passengers_inbound"
  | "passengers_outbound"
  | "flights";

export type TransportRecord = {
  period: string;
} & Record<TransportMetric, number | null>;

export type VehicleTypesMetric = "vehicles";

export type VehicleTypesRecord = {
  period: string;
  vehicle_type: string;
  vehicles: number | null;
};

export type AirTransportMeta = DatasetMetaMonthly<TransportMetric>;
export type AirTransportDataset = Dataset<TransportRecord, AirTransportMeta>;

export type VehicleTypesMeta = DatasetMetaYearly<
  VehicleTypesMetric,
  "vehicle_type"
>;
export type VehicleTypesDataset = Dataset<VehicleTypesRecord, VehicleTypesMeta>;
