export function shuffleInPlace<T>(items: T[], random: () => number = Math.random): T[] {
  for (let index = items.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(random() * (index + 1))
    const current = items[index]
    items[index] = items[swapIndex] as T
    items[swapIndex] = current as T
  }

  return items
}

export function maybeShuffle<T>(items: readonly T[], enabled: boolean): T[] {
  const copy = [...items]
  return enabled ? shuffleInPlace(copy) : copy
}
