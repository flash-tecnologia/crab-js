# Batch Size Limits Test Results

## 🔍 **Key Findings**

### **Maximum Batch Size: 10 messages**
- The library enforces a **hard limit of 10 messages per batch**
- Requests for larger batch sizes automatically fall back to 10
- This limit is enforced at the **Rust layer**, not JavaScript

### **Validation Behavior**
```
✅ Valid range: 1-10 messages
⚠️  Out of range: Automatically clamped to 10
🚨 Warning logs: "size X out of range [1-10], using 10"
```

## 📊 **Test Results Summary**

### **Boundary Conditions**
| Batch Size | Result | Warning | Description |
|------------|---------|---------|-------------|
| 1 | ✅ 1 | No | Minimum valid |
| 10 | ✅ 10 | No | Maximum valid |
| 0 | ✅ 10 | Yes | Falls back to max |
| 25 | ✅ 10 | Yes | Clamped to max |
| 1000 | ✅ 10 | Yes | Clamped to max |

### **Performance Results**
| Configuration | Throughput | Avg Batch Size |
|---------------|------------|----------------|
| Batch size 5, small messages | 280.9 msgs/sec | 5.0 |
| Batch size 10, small messages | **671.1 msgs/sec** | 10.0 |
| Batch size 10, large messages | 193.1 msgs/sec | 10.0 |

**Key Insight**: Maximum batch size (10) provides **2.4x better throughput** than smaller batches!

### **Timeout Behavior**
| Timeout | Result | Valid Range |
|---------|--------|-------------|
| 50ms | ✅ 50ms | 1-30000ms |
| 30000ms | ✅ 30000ms | Max valid |
| 0ms | ✅ 100ms | Falls back to default |
| 50000ms | ✅ 100ms | Falls back to default |

## 🛡️ **Validation Strategy**

### **JavaScript Layer**
- **No validation** - accepts any batch size value
- Validation happens during actual Rust operations
- Allows for flexible API design

### **Rust Layer** 
- **Strict enforcement** of 1-10 message limit
- Automatic fallback to valid values
- Warning logs for out-of-range requests

## 💡 **Best Practices**

### **Optimal Configuration**
```javascript
// Recommended for maximum performance
const streamConsumer = client.createStreamConsumer({
  ...consumerConfig,
  batchSize: 10,
  batchTimeout: 1000
})

// Good for balanced latency/throughput  
const streamConsumer = client.createStreamConsumer({
  ...consumerConfig,
  batchSize: 5,
  batchTimeout: 500
})

// For low-latency scenarios
const streamConsumer = client.createStreamConsumer({
  ...consumerConfig,
  batchSize: 3,
  batchTimeout: 100
})

// Single message mode (default)
const streamConsumer = client.createStreamConsumer({
  ...consumerConfig
  // batchSize defaults to undefined = single message mode
})
```

### **Performance Tuning**
1. **Use maximum batch size (10)** for highest throughput
2. **Adjust timeout** based on latency requirements:
   - High throughput: 1000-5000ms
   - Balanced: 500-1000ms  
   - Low latency: 100-500ms
3. **Consider message size** - large messages may benefit from smaller batches

## 🔬 **Technical Details**

### **Warning Pattern**
```
WARN: size {requested} out of range [1-10], using 10
```

### **Stream vs Polling Batch API**
- **Stream**: Individual messages delivered, batched internally
- **Polling**: Actual batch arrays returned up to limit
- **Both**: Subject to same 10-message maximum

### **Resource Usage**
- Larger batches reduce syscall overhead
- Memory usage scales linearly with batch size
- Network efficiency improves with larger batches (up to limit)

## 📈 **Recommendations**

1. **Default to maximum batch size (10)** unless specific requirements dictate otherwise
2. **Use timeout values 100-1000ms** for most applications  
3. **Monitor warning logs** to detect over-sized batch requests
4. **Test performance** with your specific message sizes and patterns
5. **Consider batch mode for high-throughput scenarios** (2-5x improvement possible)

## 🧪 **Test Coverage**

Our test suite validates:
- ✅ Boundary conditions (0, 1, 10, 25, 1000)
- ✅ Performance comparison across batch sizes
- ✅ Timeout validation (1-30000ms range)
- ✅ Stream and polling API consistency
- ✅ Warning message generation
- ✅ Edge cases (negative, zero, extreme values)

The batch size limit of **10 messages** is a reasonable default that balances performance, memory usage, and API simplicity.