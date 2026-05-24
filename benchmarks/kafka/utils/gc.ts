import { PerformanceObserver, constants, performance, type PerformanceEntry } from 'node:perf_hooks'

export interface GcSummary {
  activeDurationMs: number
  totalCount: number
  totalDurationMs: number
  maxDurationMs: number
  minorCount: number
  majorCount: number
  incrementalCount: number
  weakCallbackCount: number
  forcedCount: number
  otherCount: number
}

interface GcPerformanceEntryDetail {
  kind?: number
  flags?: number
}

type GcPerformanceEntry = PerformanceEntry & {
  detail?: GcPerformanceEntryDetail
}

interface GcMeasurementWindow {
  startedAtMs: number
  finishedAtMs: number
}

export function startGcObserver() {
  const summary = createEmptyGcSummary()
  const completedWindows: GcMeasurementWindow[] = []
  let activeStartedAtMs: number | undefined

  const observer = new PerformanceObserver((list) => {
    collectGcEntries(list.getEntries() as GcPerformanceEntry[])
  })

  observer.observe({ entryTypes: ['gc'] })

  function collectGcEntries(entries: readonly GcPerformanceEntry[]) {
    for (const entry of entries) {
      if (entry.entryType !== 'gc' || !isInsideMeasuredWindow(entry.startTime, activeStartedAtMs, completedWindows)) {
        continue
      }

      collectGcEntry(summary, entry)
    }
  }

  return {
    resume() {
      if (activeStartedAtMs !== undefined) {
        return
      }

      collectGcEntries(observer.takeRecords() as GcPerformanceEntry[])
      activeStartedAtMs = performance.now()
    },
    pause() {
      if (activeStartedAtMs === undefined) {
        return
      }

      const finishedAtMs = performance.now()
      collectGcEntries(observer.takeRecords() as GcPerformanceEntry[])
      summary.activeDurationMs += finishedAtMs - activeStartedAtMs
      completedWindows.push({ startedAtMs: activeStartedAtMs, finishedAtMs })
      activeStartedAtMs = undefined
    },
    reset() {
      collectGcEntries(observer.takeRecords() as GcPerformanceEntry[])
      completedWindows.length = 0
      activeStartedAtMs = undefined
      Object.assign(summary, createEmptyGcSummary())
    },
    stop(): GcSummary {
      this.pause()
      collectGcEntries(observer.takeRecords() as GcPerformanceEntry[])
      observer.disconnect()
      return { ...summary }
    },
    snapshot(): GcSummary {
      collectGcEntries(observer.takeRecords() as GcPerformanceEntry[])
      return { ...summary }
    },
  }
}

function isInsideMeasuredWindow(
  startedAtMs: number,
  activeStartedAtMs: number | undefined,
  completedWindows: readonly GcMeasurementWindow[],
): boolean {
  if (activeStartedAtMs !== undefined && startedAtMs >= activeStartedAtMs) {
    return true
  }

  return completedWindows.some((window) => startedAtMs >= window.startedAtMs && startedAtMs <= window.finishedAtMs)
}

function createEmptyGcSummary(): GcSummary {
  return {
    activeDurationMs: 0,
    totalCount: 0,
    totalDurationMs: 0,
    maxDurationMs: 0,
    minorCount: 0,
    majorCount: 0,
    incrementalCount: 0,
    weakCallbackCount: 0,
    forcedCount: 0,
    otherCount: 0,
  }
}

function collectGcEntry(summary: GcSummary, entry: GcPerformanceEntry) {
  const durationMs = Number.isFinite(entry.duration) ? Math.max(0, entry.duration) : 0
  const kind = entry.detail?.kind
  const flags = entry.detail?.flags ?? 0

  summary.totalCount += 1
  summary.totalDurationMs += durationMs
  summary.maxDurationMs = Math.max(summary.maxDurationMs, durationMs)

  switch (kind) {
    case constants.NODE_PERFORMANCE_GC_MINOR: {
      summary.minorCount += 1
      break
    }
    case constants.NODE_PERFORMANCE_GC_MAJOR: {
      summary.majorCount += 1
      break
    }
    case constants.NODE_PERFORMANCE_GC_INCREMENTAL: {
      summary.incrementalCount += 1
      break
    }
    case constants.NODE_PERFORMANCE_GC_WEAKCB: {
      summary.weakCallbackCount += 1
      break
    }
    default: {
      summary.otherCount += 1
    }
  }

  if (hasFlag(flags, constants.NODE_PERFORMANCE_GC_FLAGS_FORCED)) {
    summary.forcedCount += 1
  }
}

function hasFlag(flags: number, flag: number): boolean {
  return flags % (flag * 2) >= flag
}
