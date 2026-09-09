# RFC-0002: Auto-commit precedence

- Status: Implemented
- Review item: M02
- Priority: High

## Problem

The convenience `enableAutoCommit` option could overwrite an advanced `enable.auto.commit`
configuration even when the convenience option was omitted, making user configuration
dependent on wrapper defaults.

## Decision

Apply the convenience option only when it is explicitly supplied. Preserve the advanced
librdkafka setting when the convenience option is absent, and let an explicit convenience
value take precedence when both are supplied.

## Evidence

The regression matrix covers advanced-only, convenience-only, and conflicting configurations
in [regressions.test.ts](../../../../js-tests/unit/regressions.test.ts). Consumer configuration
assembly is implemented in [consumer_helper.rs](../../../../src/kafka/consumer/consumer_helper.rs).

## Follow-up

Keep the precedence rule documented in the public API and add effective-runtime assertions if
the binding exposes a stable way to inspect the librdkafka configuration.
