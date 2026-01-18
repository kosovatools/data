export type KqzPartyDelta = {
  total_votes_delta: number;
  candidate_deltas: Record<string, number>;
};

export type KqzPollingStationDiff = {
  municipality_id: string;
  municipality_name: string | null;
  voting_station_id: string;
  voting_station_name: string | null;
  polling_station_id: string;
  polling_station_name: string | null;
  vote_type: string;
  party_deltas: Record<string, KqzPartyDelta>;
};

export type KqzMunicipalityDiff = {
  municipality_name: string | null;
  voting_stations: Record<
    string,
    {
      voting_station_name: string | null;
      polling_stations: Record<
        string,
        {
          polling_station_name: string | null;
          vote_type: string;
          party_deltas: Record<string, KqzPartyDelta>;
        }
      >;
    }
  >;
};

export type KqzCandidateDelta = {
  party_id: string;
  party_name?: string | null;
  candidate_id: string;
  candidate_name?: string | null;
  delta: number;
};

export type KqzPartyLookup = Record<string, { name: string | null }>;
export type KqzCandidateLookup = Record<string, Record<string, { name: string | null }>>;

export type KqzRecountDiffDataset = {
  generated_at?: string | null;
  vote_type: string;
  recount_polling_station_count: number;
  missing_in_qkn: string[];
  aggregate_candidate_deltas: Record<string, Record<string, number>>;
  aggregate_candidate_deltas_flat: KqzCandidateDelta[];
  aggregate_party_deltas: Record<string, number>;
  party_lookup: KqzPartyLookup;
  candidate_lookup: KqzCandidateLookup;
  polling_station_diffs: Record<string, KqzPollingStationDiff>;
  municipality_diffs: Record<string, KqzMunicipalityDiff>;
};
