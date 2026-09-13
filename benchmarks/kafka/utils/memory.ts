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
  let processing = false
  let processingPeak: MemoryUsageSnapshot | null = null
  let processingSamples = 0
  const timer = setInterval(() => {
    const sample = readMemoryUsage()
    peak = maxMemoryUsage(peak, sample)
    if (processing) {
      processingPeak = processingPeak ? maxMemoryUsage(processingPeak, sample) : sample
      processingSamples++
    }
  }, sampleIntervalMs)

  timer.unref()

  return {
    resumeWindow() {
      processing = true
    },
    pauseWindow() {
      processing = false
    },
    processingResult() {
      return { peak: processingPeak, samples: processingSamples }
    },
    stop() {
      clearInterval(timer)
      peak = maxMemoryUsage(peak, readMemoryUsage())
      return peak
    },
  }
}

export function maxMemoryUsage(left: MemoryUsageSnapshot, right: MemoryUsageSnapshot): MemoryUsageSnapshot {
  return {
    rss: Math.max(left.rss, right.rss),
    heapUsed: Math.max(left.heapUsed, right.heapUsed),
    external: Math.max(left.external, right.external),
    arrayBuffers: Math.max(left.arrayBuffers, right.arrayBuffers),
  }
}
