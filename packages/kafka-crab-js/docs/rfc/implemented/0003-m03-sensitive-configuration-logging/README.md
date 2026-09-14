# RFC-0003: Sensitive configuration logging

- Status: Implemented
- Review item: M03
- Priority: High

## Problem

Logging complete Kafka configuration maps risks exposing SASL passwords and other credentials.
Forcing verbose consumer logging creates the same risk through indirect librdkafka output.

## Decision

Do not log complete client or consumer configuration maps. Leave logging controlled by the
requested level and rely on librdkafka diagnostics that do not print secret values.

## Evidence

A subprocess probe creates a client with a unique fake password at debug level and asserts that
the password is absent from stdout and stderr. The probe is covered by
[regressions.test.ts](../../../../js-tests/unit/regressions.test.ts) and
[credential-log-probe.mjs](../../../../js-tests/fixtures/credential-log-probe.mjs).

## Follow-up

Repeat the redaction check when new configuration fields or logging integrations are added.
