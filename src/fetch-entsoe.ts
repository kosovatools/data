#!/usr/bin/env tsx
import { mkdir, readFile, writeFile, rename, stat } from "node:fs/promises"
import path from "node:path"
import { randomUUID } from "node:crypto"
import { XMLParser } from "fast-xml-parser"

// ---- constants (vendored from the app) ----
const ENTSEO_ENDPOINT = "https://web-api.tp.entsoe.eu/api"
const KOSOVO_EIC = "10Y1001C--00100H"
const MAX_BACKFILL_MONTHS = 24
const DEFAULT_NEIGHBORS = [
    { code: "10YAL-KESH-----5", label: "Albania" },
    { code: "10YMK-MEPSO----8", label: "North Macedonia" },
    { code: "10YCS-CG-TSO---S", label: "Montenegro" },
    { code: "10YCS-SERBIATSOV", label: "Serbia" }
] as const

const parser = new XMLParser({ ignoreAttributes: false })

// ---------- utils ----------
function toEntsoeDate(date: Date) {
    const y = date.getUTCFullYear()
    const m = String(date.getUTCMonth() + 1).padStart(2, "0")
    const d = String(date.getUTCDate()).padStart(2, "0")
    return `${y}${m}${d}0000`
}
function parseResolutionToHours(resolution: unknown) {
    if (typeof resolution !== "string") return 1
    const m = resolution.match(/^PT(?:(\d+)H)?(?:(\d+)M)?$/i)
    if (!m) return 1
    const h = m[1] ? parseInt(m[1], 10) : 0
    const min = m[2] ? parseInt(m[2], 10) : 0
    return (Number.isFinite(h) ? h : 0) + (Number.isFinite(min) ? min : 0) / 60
}
function ensureArray<T>(v: T | T[] | null | undefined): T[] {
    if (!v) return []
    return Array.isArray(v) ? v : [v]
}
function extractQuantity(v: unknown): number | null {
    if (typeof v === "number") return v
    if (typeof v === "string") { const n = parseFloat(v); return Number.isFinite(n) ? n : null }
    return null
}
function parseDate(value: unknown): Date | null {
    if (typeof value !== "string") return null
    const d = new Date(value)
    return Number.isNaN(d.getTime()) ? null : d
}
function formatPeriodId(start: Date) {
    const y = start.getUTCFullYear()
    const m = String(start.getUTCMonth() + 1).padStart(2, "0")
    return `${y}-${m}`
}
function getPreviousMonthRange(reference = new Date()) {
    const end = new Date(Date.UTC(reference.getUTCFullYear(), reference.getUTCMonth(), 1))
    const start = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth() - 1, 1))
    return { start, end }
}
async function writeJsonAtomic(file: string, obj: unknown) {
    await mkdir(path.dirname(file), { recursive: true })
    const tmp = `${file}.${randomUUID()}.tmp`
    await writeFile(tmp, JSON.stringify(obj, null, 2) + "\n", "utf8")
    await rename(tmp, file)
}
async function fileExists(p: string) { try { await stat(p); return true } catch { return false } }
async function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)) }
async function safeFetch(url: string, tries = 5) {
    let delay = 1000
    for (let i = 0; i < tries; i++) {
        const res = await fetch(url)
        if (res.ok) return res
        const ra = res.headers.get("retry-after")
        if (res.status === 429 || res.status >= 500) {
            await sleep(ra ? Number(ra) * 1000 : delay); delay *= 2; continue
        }
        const msg = await res.text().catch(() => res.statusText)
        throw new Error(`ENTSO-E ${res.status}: ${msg}`)
    }
    throw new Error("ENTSO-E: exhausted retries")
}

// ---------- parsing ----------
type Sample = { timestamp: string; energyMWh: number }
function parseEnergyVolume(xml: string) {
    const doc = parser.parse(xml)
    const timeSeries = ensureArray<any>(doc?.Publication_MarketDocument?.TimeSeries)
    if (!timeSeries.length) return { energyMWh: 0, hasData: false, samples: [] as Sample[] }

    let total = 0
    let hasPoints = false
    const samples: Sample[] = []

    for (const series of timeSeries) {
        const periods = ensureArray<any>(series?.Period ?? series?.period)
        for (const p of periods) {
            const res = p?.resolution ?? p?.Resolution ?? p?.timeResolution
            const hours = parseResolutionToHours(res)
            const ms = hours * 3_600_000
            const ti = p?.timeInterval ?? p?.TimeInterval ?? {}
            const start = parseDate(ti?.start ?? ti?.Start) ?? parseDate(ti?.begin ?? ti?.Begin)
            const points = ensureArray<any>(p?.Point ?? p?.point)

            points.forEach((pt: any, idx: number) => {
                const q = extractQuantity(pt?.quantity ?? pt?.Quantity)
                if (q == null) return
                const e = q * (hours || 1)
                total += e
                hasPoints = true
                if (start && ms > 0) {
                    const posRaw = pt?.position ?? pt?.Position ?? pt?.Pos
                    const pos = parseInt(posRaw, 10)
                    const offset = Number.isFinite(pos) ? pos - 1 : idx
                    const ts = new Date(start.getTime() + offset * ms)
                    if (!Number.isNaN(ts.getTime())) samples.push({ timestamp: ts.toISOString(), energyMWh: e })
                }
            })
        }
    }

    samples.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
    return { energyMWh: total, hasData: hasPoints, samples }
}

async function fetchFlowVolume({ token, periodStart, periodEnd, inDomain, outDomain }: {
    token: string; periodStart: string; periodEnd: string; inDomain: string; outDomain: string
}) {
    const params = new URLSearchParams({
        securityToken: token, documentType: "A11",
        in_Domain: inDomain, out_Domain: outDomain, periodStart, periodEnd
    })
    const res = await safeFetch(`${ENTSEO_ENDPOINT}?${params.toString()}`)
    const xml = await res.text()
    if (!xml.trim()) return { energyMWh: 0, hasData: false, samples: [] as Sample[] }
    return parseEnergyVolume(xml)
}

// ---------- aggregation ----------
function calculateTotals(neighbors: Array<{ importMWh: number; exportMWh: number; netMWh: number }>) {
    return neighbors.reduce(
        (acc, n) => ({
            importMWh: acc.importMWh + (n.importMWh || 0),
            exportMWh: acc.exportMWh + (n.exportMWh || 0),
            netMWh: acc.netMWh + (n.netMWh || 0),
        }),
        { importMWh: 0, exportMWh: 0, netMWh: 0 }
    )
}
function sumByDay(importSamples: Sample[], exportSamples: Sample[]) {
    const map = new Map<string, { imports: number; exports: number }>()
    for (const s of importSamples) {
        const day = s.timestamp.slice(0, 10)
        const row = map.get(day) ?? { imports: 0, exports: 0 }
        row.imports += s.energyMWh || 0; map.set(day, row)
    }
    for (const s of exportSamples) {
        const day = s.timestamp.slice(0, 10)
        const row = map.get(day) ?? { imports: 0, exports: 0 }
        row.exports += s.energyMWh || 0; map.set(day, row)
    }
    return [...map.entries()].sort(([a], [b]) => a.localeCompare(b))
        .map(([date, v]) => ({ date, imports: v.imports, exports: v.exports, net: v.imports - v.exports }))
}

async function createSnapshot({ token, start, end }: { token: string; start: Date; end: Date }) {
    const snapshotId = formatPeriodId(start)
    const periodStart = toEntsoeDate(start)
    const periodEnd = toEntsoeDate(end)

    const neighbors: any[] = []
    const allImport: Sample[] = []
    const allExport: Sample[] = []

    for (const n of DEFAULT_NEIGHBORS) {
        const imp = await fetchFlowVolume({ token, periodStart, periodEnd, inDomain: KOSOVO_EIC, outDomain: n.code })
        const exp = await fetchFlowVolume({ token, periodStart, periodEnd, inDomain: n.code, outDomain: KOSOVO_EIC })

        neighbors.push({
            code: n.code, country: n.label,
            importMWh: imp.energyMWh ?? 0,
            exportMWh: exp.energyMWh ?? 0,
            netMWh: (imp.energyMWh ?? 0) - (exp.energyMWh ?? 0),
            hasData: Boolean(imp.hasData || exp.hasData),
        })

        allImport.push(...imp.samples)
        allExport.push(...exp.samples)
    }

    neighbors.sort((a, b) => b.netMWh - a.netMWh)
    const totals = calculateTotals(neighbors)
    const daily = sumByDay(allImport, allExport)

    return {
        monthly: { id: snapshotId, periodStart: start.toISOString(), periodEnd: end.toISOString(), neighbors, totals },
        latestDaily: { snapshotId, periodStart: start.toISOString(), periodEnd: end.toISOString(), days: daily }
    }
}

// ---------- CLI ----------
async function main() {
    const token = process.env.ENTSOE_API_KEY;
    if (!token) { console.error("Missing ENTSOE_API_KEY"); process.exit(1) }

    const args = new Map<string, string>()
    const cli = process.argv.slice(2)
    for (let i = 0; i < cli.length; i++) {
        const raw = cli[i]
        if (raw.startsWith("--")) {
            if (raw.includes("=")) {
                const [k, v = ""] = raw.split("=")
                args.set(k, v)
            } else {
                const next = cli[i + 1]
                if (next && !next.startsWith("--")) {
                    args.set(raw, next)
                    i++
                } else {
                    args.set(raw, "true")
                }
            }
        } else {
            args.set(raw, "true")
        }
    }
    const outDir = path.resolve(args.get("--out") ?? "./data/energy")
    const monthArg = args.get("--month")
    const backfillArg = args.get("--backfill") ?? args.get("--months")

    const baseRange = monthArg && /^\d{4}-\d{2}$/.test(monthArg)
        ? (() => { const [y, m] = monthArg.split("-").map(Number); return { start: new Date(Date.UTC(y, m - 1, 1)), end: new Date(Date.UTC(y, m, 1)) } })()
        : getPreviousMonthRange()

    let backfillCount = 1
    if (backfillArg) {
        const parsed = parseInt(backfillArg, 10)
        if (Number.isFinite(parsed) && parsed > 0) {
            if (parsed > MAX_BACKFILL_MONTHS) {
                console.warn(`Capping backfill to ${MAX_BACKFILL_MONTHS} months to limit API load.`)
            }
            backfillCount = Math.min(parsed, MAX_BACKFILL_MONTHS)
        }
    }

    const months: Array<{ start: Date; end: Date }> = []
    for (let offset = backfillCount - 1; offset >= 0; offset--) {
        const start = new Date(Date.UTC(baseRange.start.getUTCFullYear(), baseRange.start.getUTCMonth() - offset, 1))
        const end = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth() + 1, 1))
        months.push({ start, end })
    }

    const latestDailyPath = path.join(outDir, "latest-daily.json")
    let touchedAny = false

    for (let i = 0; i < months.length; i++) {
        const { start, end } = months[i]
        const id = formatPeriodId(start)
        const isNewest = i === months.length - 1
        const monthlyPath = path.join(outDir, "monthly", `${id}.json`)

        if (await fileExists(monthlyPath)) {
            console.log(`Month ${id} already present, refreshing pointers...`)
            const existing = JSON.parse(await readFile(monthlyPath, "utf8"))
            await updatePointers(outDir, existing)
            continue
        }

        const { monthly, latestDaily } = await createSnapshot({ token, start, end })

        await writeJsonAtomic(monthlyPath, monthly)
        await updatePointers(outDir, monthly)
        if (isNewest) {
            await writeJsonAtomic(latestDailyPath, latestDaily)
        }

        const suffix = isNewest ? " + latest daily" : ""
        console.log(`Saved ${id} monthly${suffix}.`)
        touchedAny = true
    }

    if (!touchedAny) {
        console.log("No new months fetched. Existing data kept in place.")
    }
}

async function updatePointers(outDir: string, monthly: { id: string; periodStart: string; periodEnd: string; totals: any }) {
    const indexPath = path.join(outDir, "index.json")
    let index: any = { generatedAt: new Date().toISOString(), months: [] as any[] }
    try {
        const raw = await readFile(indexPath, "utf8")
        const parsed = JSON.parse(raw)
        if (parsed && typeof parsed === "object" && Array.isArray(parsed.months)) index = parsed
    } catch { }
    const minimal = { id: monthly.id, periodStart: monthly.periodStart, periodEnd: monthly.periodEnd, totals: monthly.totals }
    index.months = [
        ...index.months.filter((m: any) => m?.id !== monthly.id),
        minimal
    ].sort((a: any, b: any) => new Date(a.periodStart).getTime() - new Date(b.periodStart).getTime())
    index.generatedAt = new Date().toISOString()
    await writeJsonAtomic(indexPath, index)

    await writeJsonAtomic(path.join(outDir, "latest.json"), {
        snapshotId: monthly.id, periodStart: monthly.periodStart, periodEnd: monthly.periodEnd
    })
}

main().catch(err => { console.error(err); process.exit(1) })
