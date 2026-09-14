// Optional instrumentation for the real benchmark's measured message window.
export function createSerialTiming(count: number, mode: 'cpu' | 'gaps') {
  const gaps = mode === 'gaps' ? new Float64Array(Math.max(0, count - 1)) : undefined
  let used = 0
  let last: number | undefined
  let startedAtMs = 0
  let wallMs = 0
  let firstDeliveryGapMs: number | undefined
  let cpuStart: NodeJS.CpuUsage
  let threadStart: NodeJS.CpuUsage
  let cpu: NodeJS.CpuUsage
  let thread: NodeJS.CpuUsage

  return {
    start() {
      cpuStart = process.cpuUsage()
      threadStart = process.threadCpuUsage()
      startedAtMs = performance.now()
    },
    delivery: gaps
      ? () => {
          const now = performance.now()
          firstDeliveryGapMs ??= now - startedAtMs
          if (last !== undefined) gaps[used++] = now - last
          last = now
        }
      : undefined,
    finish() {
      wallMs = performance.now() - startedAtMs
      thread = process.threadCpuUsage(threadStart)
      cpu = process.cpuUsage(cpuStart)
    },
    result() {
      const stalls = gaps
        ? [0.1, 1, 5].map((thresholdMs) => {
            let events = 0
            let totalMs = 0
            for (let i = 0; i < used; i++) {
              const gap = gaps[i] ?? 0
              if (gap >= thresholdMs) {
                events++
                totalMs += gap
              }
            }
            return { thresholdMs, events, totalMs }
          })
        : undefined
      const sorted = gaps?.subarray(0, used).toSorted()
      return {
        mode,
        wallMs,
        processCpuMs: (cpu.user + cpu.system) / 1000,
        mainThreadCpuMs: (thread.user + thread.system) / 1000,
        // Includes scheduling, GC and blocking; this is NOT Kafka I/O time.
        mainThreadNonCpuMs: Math.max(0, wallMs - (thread.user + thread.system) / 1000),
        firstDeliveryGapMs,
        gapCount: gaps ? used : undefined,
        gapP99Ms: sorted?.[Math.floor(used * 0.99)],
        gapMaxMs: sorted?.at(-1),
        stalls,
      }
    },
  }
}

export type SerialTimingResult = ReturnType<ReturnType<typeof createSerialTiming>['result']>
