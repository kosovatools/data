#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_recount_metadata_lookup(metadata, vote_type="1"):
    lookup = {}
    for municipality in metadata.get("municipalities", []):
        municipality_id = municipality.get("id")
        vote_types = municipality.get("vote_types", {})
        recount_vote_type = vote_types.get(vote_type)
        if not recount_vote_type:
            continue
        for voting_station in recount_vote_type.get("votingStations", []):
            voting_station_id = voting_station.get("id")
            for polling_station in voting_station.get("pollingStations", []):
                polling_station_id = polling_station.get("id")
                lookup[polling_station_id] = {
                    "municipality_id": municipality_id,
                    "municipality_name": municipality.get("name"),
                    "voting_station_id": voting_station_id,
                    "voting_station_name": voting_station.get("name"),
                    "polling_station_id": polling_station_id,
                    "polling_station_name": polling_station.get("name"),
                    "vote_type": str(polling_station.get("vote_type")),
                }
    return lookup




def build_municipality_lookup(metadata):
    return {mun.get("id"): mun.get("name") for mun in metadata.get("municipalities", [])}


def build_candidate_lookup(full_candidates):
    party_lookup = {}
    candidate_lookup = {}
    for party_id, party_info in full_candidates.items():
        party_lookup[party_id] = {"name": party_info.get("name")}
        candidate_lookup[party_id] = {}
        candidates = party_info.get("0", {})
        for candidate_id, candidate_info in candidates.items():
            candidate_lookup[party_id][candidate_id] = {
                "name": candidate_info.get("name")
            }
    return party_lookup, candidate_lookup


def flatten_qkn_polling_stations(qkn):
    stations = {}
    for municipality_id, municipality in qkn.get("municipalities", {}).items():
        for voting_station_id, voting_station in municipality.get(
            "voting_stations", {}
        ).items():
            for polling_station_id, polling_station in voting_station.get(
                "polling_stations", {}
            ).items():
                stations[polling_station_id] = {
                    "municipality_id": municipality_id,
                    "voting_station_id": voting_station_id,
                    "summary": polling_station.get("summary", {}),
                }
    return stations


def iter_qnr_recount_polling_stations(qnr, vote_type="1"):
    for municipality_id, municipality in qnr.get("municipalities", {}).items():
        vote_types = municipality.get("vote_types", {})
        recount_vote_type = vote_types.get(vote_type)
        if not recount_vote_type:
            continue
        for voting_station_id, voting_station in recount_vote_type.get(
            "voting_stations", {}
        ).items():
            for polling_station_id, polling_station in voting_station.get(
                "polling_stations", {}
            ).items():
                yield {
                    "municipality_id": municipality_id,
                    "voting_station_id": voting_station_id,
                    "polling_station_id": polling_station_id,
                    "summary": polling_station.get("summary", {}),
                }


def diff_summary(recount_summary, first_summary):
    party_diffs = {}
    all_party_ids = set(recount_summary.keys()) | set(first_summary.keys())
    for party_id in sorted(all_party_ids, key=lambda x: int(x)):
        recount_party = recount_summary.get(party_id, {})
        first_party = first_summary.get(party_id, {})
        recount_total = recount_party.get("total_votes", 0)
        first_total = first_party.get("total_votes", 0)
        total_delta = recount_total - first_total

        recount_candidates = recount_party.get("candidates", {})
        first_candidates = first_party.get("candidates", {})
        candidate_ids = set(recount_candidates.keys()) | set(first_candidates.keys())
        candidate_deltas = {}
        for candidate_id in sorted(candidate_ids, key=lambda x: int(x)):
            delta = recount_candidates.get(candidate_id, 0) - first_candidates.get(
                candidate_id, 0
            )
            if delta != 0:
                candidate_deltas[candidate_id] = delta

        if total_delta != 0 or candidate_deltas:
            party_diffs[party_id] = {
                "total_votes_delta": total_delta,
                "candidate_deltas": candidate_deltas,
            }
    return party_diffs


def main():
    data_dir = Path(__file__).resolve().parents[3] / "data" / "kqz"
    parser = argparse.ArgumentParser(
        description="Diff QKN vs QNR summaries for recount polling stations (vote_type=1)."
    )
    parser.add_argument(
        "--qkn",
        type=Path,
        default=data_dir / "parliamentary-qkn-latest.json",
        help="Path to QKN (first count) JSON",
    )
    parser.add_argument(
        "--qnr",
        type=Path,
        default=data_dir / "parliamentary-qnr-latest.json",
        help="Path to QNR (recount) JSON",
    )
    parser.add_argument(
        "--vote-type",
        default="1",
        help="Vote type to treat as recount (default: 1)",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=data_dir / "qnr-metadata-parliamentary.json",
        help="Path to QNR metadata for station names",
    )
    parser.add_argument(
        "--candidates",
        type=Path,
        default=data_dir / "full-candidates-parliamentary.json",
        help="Path to full candidates list for names",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=data_dir / "qkn_qnr_recount_diff.json",
        help="Output path for diff JSON",
    )
    args = parser.parse_args()

    qkn = load_json(args.qkn)
    qnr = load_json(args.qnr)
    metadata = load_json(args.metadata) if args.metadata.exists() else {}
    candidates_data = load_json(args.candidates) if args.candidates.exists() else {}

    qkn_stations = flatten_qkn_polling_stations(qkn)
    recount_lookup = build_recount_metadata_lookup(metadata, args.vote_type)
    municipality_lookup = build_municipality_lookup(metadata)
    party_lookup, candidate_lookup = build_candidate_lookup(candidates_data)

    aggregate_party_deltas = defaultdict(int)
    aggregate_candidate_deltas = defaultdict(lambda: defaultdict(int))
    polling_station_diffs = {}
    municipality_diffs = {}
    missing_in_qkn = []

    recount_stations = list(iter_qnr_recount_polling_stations(qnr, args.vote_type))
    for recount_station in recount_stations:
        polling_station_id = recount_station["polling_station_id"]
        recount_summary = recount_station["summary"]
        first_summary = qkn_stations.get(polling_station_id, {}).get("summary", {})
        if polling_station_id not in qkn_stations:
            missing_in_qkn.append(polling_station_id)

        party_diffs = diff_summary(recount_summary, first_summary)
        if party_diffs:
            metadata_entry = recount_lookup.get(
                polling_station_id,
                {
                    "municipality_id": recount_station["municipality_id"],
                    "municipality_name": municipality_lookup.get(recount_station["municipality_id"]),
                    "voting_station_id": recount_station["voting_station_id"],
                    "voting_station_name": None,
                    "polling_station_id": polling_station_id,
                    "polling_station_name": None,
                    "vote_type": args.vote_type,
                },
            )
            polling_station_diffs[polling_station_id] = {
                "municipality_id": metadata_entry["municipality_id"],
                "municipality_name": metadata_entry["municipality_name"],
                "voting_station_id": metadata_entry["voting_station_id"],
                "voting_station_name": metadata_entry["voting_station_name"],
                "polling_station_id": metadata_entry["polling_station_id"],
                "polling_station_name": metadata_entry["polling_station_name"],
                "vote_type": metadata_entry["vote_type"],
                "party_deltas": party_diffs,
            }

            mun_id = metadata_entry["municipality_id"]
            vs_id = metadata_entry["voting_station_id"]
            ps_id = metadata_entry["polling_station_id"]
            municipality_entry = municipality_diffs.setdefault(
                mun_id,
                {
                    "municipality_name": metadata_entry["municipality_name"],
                    "voting_stations": {},
                },
            )
            voting_station_entry = municipality_entry["voting_stations"].setdefault(
                vs_id,
                {
                    "voting_station_name": metadata_entry["voting_station_name"],
                    "polling_stations": {},
                },
            )
            voting_station_entry["polling_stations"][ps_id] = {
                "polling_station_name": metadata_entry["polling_station_name"],
                "vote_type": metadata_entry["vote_type"],
                "party_deltas": party_diffs,
            }

        for party_id, diff in party_diffs.items():
            aggregate_party_deltas[party_id] += diff["total_votes_delta"]
            for candidate_id, delta in diff["candidate_deltas"].items():
                aggregate_candidate_deltas[party_id][candidate_id] += delta

    aggregate_candidate_deltas_flat = []
    for party_id, candidates in aggregate_candidate_deltas.items():
        for candidate_id, delta in candidates.items():
            if delta != 0:
                aggregate_candidate_deltas_flat.append(
                    {
                        "party_id": party_id,
                        "party_name": party_lookup.get(party_id, {}).get("name"),
                        "candidate_id": candidate_id,
                        "candidate_name": candidate_lookup.get(party_id, {})
                        .get(candidate_id, {})
                        .get("name"),
                        "delta": delta,
                    }
                )

    output_payload = {
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "vote_type": args.vote_type,
        "recount_polling_station_count": len(recount_stations),
        "missing_in_qkn": sorted(set(missing_in_qkn)),
        "aggregate_candidate_deltas": {
            party_id: dict(candidates)
            for party_id, candidates in aggregate_candidate_deltas.items()
        },
        "aggregate_candidate_deltas_flat": aggregate_candidate_deltas_flat,
        "aggregate_party_deltas": dict(aggregate_party_deltas),
        "party_lookup": party_lookup,
        "candidate_lookup": candidate_lookup,
        "polling_station_diffs": polling_station_diffs,
        "municipality_diffs": municipality_diffs,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Recount polling stations: {len(recount_stations)}")
    print(f"Missing in QKN: {len(set(missing_in_qkn))}")
    print(f"Diff output: {args.output}")


if __name__ == "__main__":
    main()
