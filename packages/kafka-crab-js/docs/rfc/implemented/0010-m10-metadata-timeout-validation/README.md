# RFC-0010: Metadata timeout validation

- Status: Implemented
- Review item: M10
- Priority: Medium

## Problem

Casting a negative `fetchMetadataTimeout` directly to `u64` converted invalid input into an
extremely large duration.

## Decision

Reject negative values before conversion. Preserve the documented default for omitted values,
and normalize zero to the supported fallback rather than creating an invalid duration.

## Evidence

Regressions require negative input to fail and zero to be accepted (fallback by normalization;
the effective default value is not observable through `getConfig`, so the test pins acceptance,
not the resulting duration). Validation is implemented in
[kafka_consumer.rs](../../../../src/kafka/consumer/kafka_consumer.rs) and the consumer model.

## Follow-up

Document upper bounds and measure metadata operations with a real broker; see
[RFC-0014](../../proposed/0014-metadata-fetch-offload/).
