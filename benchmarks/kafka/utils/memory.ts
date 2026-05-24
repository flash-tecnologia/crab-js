export interface MemoryUsageSnapshot {
  rss: number
  heapUsed: number
  external: number
  arrayBuffers: number
}

export function readMemoryUsage(): MemoryUsageSnapshot {
  const usage = process.memoryUsage()

  return {
    rss: usage.rss,
    heapUsed: usage.heapUsed,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
  }
}

export function diffMemoryUsage(left: MemoryUsageSnapshot, right: MemoryUsageSnapshot): MemoryUsageSnapshot {
  return {
    rss: left.rss - right.rss,
    heapUsed: left.heapUsed - right.heapUsed,
    external: left.external - right.external,
    arrayBuffers: left.arrayBuffers - right.arrayBuffers,
  }
}

export function startMemorySampler(sampleIntervalMs: number) {
  let peak = readMemoryUsage()
  const timer = setInterval(() => {
    peak = maxMemoryUsage(peak, readMemoryUsage())
  }, sampleIntervalMs)

  timer.unref()

  return {
    stop() {
      clearInterval(timer)
      peak = maxMemoryUsage(peak, readMemoryUsage())
      return peak
    },
  }
}

function maxMemoryUsage(left: MemoryUsageSnapshot, right: MemoryUsageSnapshot): MemoryUsageSnapshot {
  return {
    rss: Math.max(left.rss, right.rss),
    heapUsed: Math.max(left.heapUsed, right.heapUsed),
    external: Math.max(left.external, right.external),
    arrayBuffers: Math.max(left.arrayBuffers, right.arrayBuffers),
  }
}
