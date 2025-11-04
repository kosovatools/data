#!/usr/bin/env tsx
import { access, mkdir, readFile, stat, writeFile } from "node:fs/promises"
import path from "node:path"

const SOURCE = process.env.CUSTOMS_DATA_SOURCE_URL || ""
const DATA_PATH = path.resolve("data/customs/tarrifs.json")

const KEEP_FIELDS = new Set([
    "code",
    "description",
    "percentage",
    "cefta",
    "msa",
    "trmtl",
    "tvsh",
    "excise",
    "validFrom",
    "uomCode"
])
const NULLABLE_FIELDS = new Set(["uomCode"])
const NUMBER_FIELDS = new Set(["percentage", "cefta", "msa", "trmtl", "tvsh", "excise"])
const STRING_FIELDS = new Set(["code", "description", "validFrom", "uomCode"])

type OptionalCounts = Record<string, number>

async function fileExists(filePath: string) {
    try {
        await access(filePath)
        return true
    } catch {
        return false
    }
}

function normalizeValue(key: string, value: unknown) {
    if (NUMBER_FIELDS.has(key)) {
        const num = Number(value)
        return Number.isFinite(num) ? num : 0
    }
    if (STRING_FIELDS.has(key)) {
        if (value === undefined || value === null) return ""
        return String(value)
    }
    return value ?? null
}

function trimRecord(record: any, index: number, optionalDropCounts: OptionalCounts) {
    if (record === null || typeof record !== "object") {
        throw new TypeError(
            `Expected object record at index ${index}, received ${typeof record}`
        )
    }
    const trimmed: Record<string, unknown> = {}
    for (const field of KEEP_FIELDS) {
        const value = (record as Record<string, unknown>)[field]
        if ((value === undefined || value === null) && NULLABLE_FIELDS.has(field)) {
            optionalDropCounts[field] = (optionalDropCounts[field] ?? 0) + 1
            continue
        }
        trimmed[field] = normalizeValue(field, value)
    }
    const code = trimmed.code
    if (typeof code !== "string" || code.length === 0) {
        throw new TypeError(`Record at index ${index} is missing required string code`)
    }
    return trimmed
}

function formatBytes(size: number) {
    const units = ["B", "KB", "MB", "GB"]
    let value = size
    let unitIndex = 0
    while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024
        unitIndex += 1
    }
    const precision = unitIndex === 0 ? 0 : 2
    return `${value.toFixed(precision)} ${units[unitIndex]}`
}

async function fetchTariffs({
    sourceUrl,
    outputPath
}: {
    sourceUrl: string
    outputPath: string
}) {
    if (!sourceUrl) {
        throw new Error("CUSTOMS_DATA_SOURCE_URL environment variable is required")
    }

    console.log(`Fetching customs data from ${sourceUrl}`)
    const response = await fetch(sourceUrl)
    if (!response.ok) {
        const message = await response.text().catch(() => response.statusText)
        throw new Error(`Failed to download dataset: ${response.status} ${message}`)
    }

    const payload = await response.text()
    let parsed: unknown
    try {
        parsed = JSON.parse(payload)
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        throw new Error(`Received invalid JSON payload: ${message}`)
    }

    if (!Array.isArray(parsed)) {
        throw new Error(`Expected JSON array but received ${typeof parsed}`)
    }

    await mkdir(path.dirname(outputPath), { recursive: true })
    await writeFile(outputPath, JSON.stringify(parsed), "utf8")
    console.log(`Saved ${parsed.length} records to ${path.relative(process.cwd(), outputPath)}`)

    return { count: parsed.length }
}

async function trimTariffs({ inputPath }: { inputPath: string }) {
    const [beforeStat, raw] = await Promise.all([stat(inputPath), readFile(inputPath, "utf8")])

    let parsed: unknown
    try {
        parsed = JSON.parse(raw)
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        throw new Error(`Failed to parse JSON payload: ${message}`)
    }

    if (!Array.isArray(parsed)) {
        throw new TypeError(`Expected tariff data to be an array but received ${typeof parsed}`)
    }

    const optionalDropCounts: OptionalCounts = {}
    const trimmed = parsed.map((record, index) =>
        trimRecord(record, index, optionalDropCounts)
    ) as Array<Record<string, any>>

    const lastIndexByCode = new Map<string, number>()
    for (const [index, record] of trimmed.entries()) {
        lastIndexByCode.set(String(record.code), index)
    }

    const deduped = trimmed.filter(
        (record, index) => lastIndexByCode.get(String(record.code)) === index
    )

    await writeFile(inputPath, JSON.stringify(deduped), "utf8")
    const afterStat = await stat(inputPath)

    const firstRecord = (parsed[0] ?? {}) as Record<string, unknown>
    const removedFields = Object.keys(firstRecord).filter((key) => !KEEP_FIELDS.has(key))

    console.log(
        `Trimmed ${parsed.length} records. Removed fields: ${removedFields.length ? removedFields.join(", ") : "none"
        }.`
    )
    if (Object.keys(optionalDropCounts).length) {
        const optionalSummary = Object.entries(optionalDropCounts)
            .map(([field, count]) => `${field}=${count}`)
            .join(", ")
        console.log(`Omitted null fields: ${optionalSummary}`)
    }
    const duplicateCount = trimmed.length - deduped.length
    if (duplicateCount > 0) {
        console.log(`Deduplicated ${duplicateCount} records by keeping latest codes.`)
    }
    console.log(`Size: ${formatBytes(beforeStat.size)} -> ${formatBytes(afterStat.size)}`)
}

async function main() {
    if (!SOURCE) {
        console.log(
            "Skipping customs tariff fetch because CUSTOMS_DATA_SOURCE_URL is not configured."
        )
        process.exit(0)
    }

    let fetched = false
    try {
        await fetchTariffs({ sourceUrl: SOURCE, outputPath: DATA_PATH })
        fetched = true
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        console.warn(`Failed to fetch updated customs data (${message}). Reusing the existing dataset.`)
    }

    if (!fetched) {
        const hasExisting = await fileExists(DATA_PATH)
        if (!hasExisting) {
            console.error(
                `Unable to reuse customs dataset because ${path.relative(
                    process.cwd(),
                    DATA_PATH
                )} does not exist.`
            )
            process.exit(1)
        }
    }

    try {
        await trimTariffs({ inputPath: DATA_PATH })
    } catch (error) {
        console.error("Failed to trim customs data:", error)
        process.exit(1)
    }
}

main().catch((error) => {
    console.error("Unexpected error while refreshing customs data:", error)
    process.exit(1)
})
