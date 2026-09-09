# Análise técnica e TODOs — kafka-crab-js

Análise inicial: 8 de setembro de 2026  
Última revisão: 9 de setembro de 2026 (revisão de caça a falhas incorporada)
Escopo: pacote `packages/kafka-crab-js`, testes, CI e benchmarks relacionados.  
Referência: commit `05ba800` e alterações locais ainda não commitadas.

The English RFC index is available at [docs/rfc/README.md](docs/rfc/README.md), with implemented
items separated from proposed follow-up work.

## Como acompanhar

Os itens com `[x]` representam mudanças observadas no código ou verificações já executadas, conforme descrito em cada item. Os itens com `[ ]` são TODOs pendentes. Uma correção no escopo original pode estar concluída e ainda precisar de testes adicionais ou documentação.

Esta revisão substitui o diagnóstico inicial como referência do estado atual. Dos dez pontos acompanhados (M01–M10), todos têm correção no escopo identificado e cobertura no caminho feliz. Restam brechas fora do caminho feliz (concorrência, erros de broker, contratos de borda), listadas em "Brechas restantes". A arquitetura Rust + TypeScript, a instrumentação por `diagnostics_channel` e os benchmarks em processos isolados continuam sendo bases adequadas para o projeto.

## Situação dos pontos originais

| ID  | Prioridade original | Ponto                                  | Situação                                                                             |
| --- | ------------------- | -------------------------------------- | ------------------------------------------------------------------------------------ |
| M01 | Alta                | Filas ilimitadas e backpressure        | Corrigido; cancelamento nativo encerra stream ociosa sem consumir mensagens futuras  |
| M02 | Alta                | Precedência de auto-commit             | Corrigido no código; teste cobre precedência como não-validação em 1 caso (parcial)  |
| M03 | Alta                | Configurações sensíveis nos logs       | Corrigido; probes confirmam ausência de credenciais em logs de client e consumer     |
| M04 | Alta                | Atribuição de múltiplos tópicos        | Corrigido; múltiplos tópicos e partições preservados na atribuição única             |
| M05 | Alta                | Preservação de tombstones              | Corrigido; distinction Buffer vazio / tombstone validada em direto, compacto e diag  |
| M06 | Alta                | Falhas parciais e retenção no producer | Corrigido; spawn_blocking para flush, callbacks tardios descartados, falhas expostas |
| M07 | Média               | Propagação de erros                    | Corrigido; falhas fatais de criação propagam em subscribe() e commit assíncrono      |
| M08 | Média               | Modo de objetos nas streams Node       | Corrigido; highWaterMark personalizado preserva objectMode                           |
| M09 | Média               | Erro após primeira mensagem            | Corrigido; batch preserva mensagem válida e propaga erro na leitura subsequente      |
| M10 | Média               | Timeout de metadados negativo          | Corrigido; valores negativos rejeitados no construtor do consumer                    |

## Correções concluídas no escopo original

- [x] **M02 — Preservar a configuração avançada de auto-commit.** `enableAutoCommit` só sobrescreve `enable.auto.commit` quando fornecido explicitamente. Na ausência das duas opções, permanece o padrão do librdkafka. Verificado por inspeção de [consumer_helper.rs](src/kafka/consumer/consumer_helper.rs) e testes de regressão em [regressions.test.ts](js-tests/unit/regressions.test.ts).
- [x] **M03 — Remover os logs completos de configuração identificados.** Foram removidos os mapas registrados em `info` e `debug`, assim como o nível `Debug` forçado no consumer. Verificado por teste automatizado de subprocesso em [regressions.test.ts](js-tests/unit/regressions.test.ts).
- [x] **M04 — Consolidar atribuições em uma única lista.** `subscribe()` monta uma `TopicPartitionList` antes de chamar `assign()`. A reprodução local com dois tópicos e offsets explícitos preservou ambos. Ver [kafka_consumer.rs](src/kafka/consumer/kafka_consumer.rs), função `subscribe`.
- [x] **M05 — Distinguir tombstones de buffers vazios.** O consumer mantém `payload: Buffer` e adiciona `isTombstone`; o batch compacto transporta `tombstones`. O producer permite payload ausente ou `isTombstone: true`. Testes pontuais com mock do librdkafka preservaram buffer vazio, tombstone e conteúdo normal nos caminhos direto, compacto e diagnostics. Ver [model.rs](src/kafka/producer/model.rs), [kafka_util.rs](src/kafka/kafka_util.rs) e [kafka-client.ts](js-src/kafka-client.ts).
- [x] **M08 — Garantir modo de objetos.** A fábrica e a classe base aplicam `objectMode: true` e rejeitam `objectMode: false`. A reprodução com `streamOptions: { highWaterMark: 4 }` confirmou `readableObjectMode === true`. Ver [kafka-client.ts](js-src/kafka-client.ts) e [base-kafka-stream-readable.ts](js-src/streams/base-kafka-stream-readable.ts).

A solução de M05 preserva a distinção sem exigir `payload: null` na saída. A documentação deve refletir essa escolha, em vez da proposta inicial de alterar o tipo do payload recebido.

## Bugs e melhorias implementados

### M01 — Cancelamento e encerramento das streams

**Prioridade: alta. Status: corrigido e testado.**

Evidência: [kafka_consumer.rs](src/kafka/consumer/kafka_consumer.rs), funções `recv_batch_stream_internal`, `recv_batch_stream_compact_internal` e `recv_stream` (linhas ~814–1258).

- [x] Substituir os canais ilimitados por `mpsc::channel` com capacidade de quatro batches.
- [x] Observar o sinal de desconexão durante a segunda fase de preenchimento do batch.
- [x] **Interromper a coleta nativa quando o leitor é cancelado.** Wrapper em Rust repassa evento de cancelamento para canal `watch::channel<bool>`, abortando tanto a fase de coleta no Kafka quanto o loop de envio nativo. Implementado em `recv_batch_stream`, `recv_batch_stream_compact` e `recv_stream` (stream serial).
- [x] **Evitar uma segunda espera por desconexão após o sinal já ter sido consumido.** Uso de checks biased em `select!` com sinais de cancelamento e desconexão.
- [x] **Tornar o envio de erros cancelável.** Envio de erro observa desconexão e cancelamento via `select!`.
- [x] Adicionar regressões para cancelamento com tópico vazio, leitura pendente, batch parcial, cancelamento de stream serial e fila de prefetch.

### M06 — Resultados tardios e bloqueio do runtime no producer

**Prioridade: alta, incluindo concorrência. Status: corrigido e testado.**

Evidência: [kafka_producer.rs](src/kafka/producer/kafka_producer.rs), callback `delivery`, métodos `send` e `flush`, e funções de coleta de resultados.

- [x] Armazenar metadados de entrega em vez de manter um `OwnedMessage` completo por resultado.
- [x] Informar a quantidade de mensagens enfileiradas quando ocorre falha parcial.
- [x] Tentar recolher resultados das mensagens já aceitas no caminho de falha parcial com auto-flush.
- [x] Substituir a sequência de iteração seguida de `clear()` no flush manual por remoção individual das chaves observadas.
- [x] **Tratar callbacks posteriores ao timeout sem vazamento de memória.** Unificação em mapa único `DashMap<String, MessageDeliveryState>` (`Pending` | `Delivered`). O callback `delivery()` só atualiza se `Occupied`, descartando callbacks tardios se o registro já expirou e foi removido.
- [x] **Evitar descarte de confirmações de envios concorrentes durante flush manual.** Remoção de `clear_all_pending_ids()`. O flush fotografa as chaves pendentes do lote atual (`target_keys`) e remove estritamente essas chaves, preservando envios concorrentes adicionados durante o flush.
- [x] **Preservar resultados em falhas parciais.** Exposição de `getLastDeliveryResults()` no producer e tipagem enriquecida `SendFailureError` no JavaScript contendo `enqueuedCount`, `totalCount`, `confirmedCount` e `confirmedMessages: RecordMetadata[]`.
- [x] **Preservar contrato de flush público.** Mantida compatibilidade com contrato existente (`autoFlush: true` retorna `[]`).
- [x] **Retirar o flush bloqueante dos workers assíncronos Tokio.** Uso de `tokio::task::spawn_blocking` para operações de flush.
- [x] Definir um contrato estruturado para falhas parciais (`enqueued X of Y, confirmed Z`).
- [x] Adicionar regressões para timeout com callback tardio, fila cheia após aceitação parcial, flush concorrente com send e bloqueio de timers Tokio.

### M07 — Propagar falhas de criação até o chamador

**Prioridade: média. Status: corrigido e testado.**

Evidência: [context.rs](src/kafka/consumer/context.rs), `commit_callback`; [kafka_admin.rs](src/kafka/kafka_admin.rs), `create_topic`; [kafka_consumer.rs](src/kafka/consumer/kafka_consumer.rs), `subscribe`, aproximadamente linha 843.

- [x] Preencher o erro no evento de callback de commit.
- [x] Verificar os resultados individuais de `create_topics()`.
- [x] Restringir os erros tolerados pelo helper de criação a tópico já existente.
- [x] **Propagar erros fatais de criação em `subscribe()`.** Falhas não recuperáveis lançam erro e interrompem a assinatura.
- [x] Adicionar regressões para broker inacessível e falha fatal de criação em `subscribe()`.

### M09 — Não descartar erros durante o preenchimento do batch

**Prioridade: média. Status: corrigido e testado.**

Evidência: [kafka_consumer.rs](src/kafka/consumer/kafka_consumer.rs), segunda fase de `collect_batch_messages` e `collect_batch_messages_compact`.

- [x] **Preservar e encaminhar o erro recebido após a primeira mensagem.** Mensagens válidas já coletadas são retornadas primeiro e o erro é armazenado em `pending_error`, sendo propagado na chamada/pull subsequente.
- [x] Preservar as mensagens válidas já coletadas ao implementar a propagação do erro.
- [x] Cobrir a sequência mensagem válida → erro nos coletores diretos e no caminho compacto `recvBatchStreamCompact` (validado com `enable.partition.eof: 'true'`).
- [x] Validar que o erro em Phase B é preservado e retornado imediatamente sem nova espera ou polling.

### M10 — Validar o timeout de metadados

**Prioridade: média. Status: corrigido e testado.**

Evidência: [kafka_consumer.rs](src/kafka/consumer/kafka_consumer.rs), construtor do consumer.

- [x] **Rejeitar valores negativos antes da conversão.** Lança erro explícito se `fetchMetadataTimeout < 0`.
- [x] Adicionar testes de fronteira rejeitando valores negativos.

**Critério de conclusão:** configurações inválidas não produzem durações enormes por conversão numérica. Achado identificado por inspeção, sem executar uma espera com timeout negativo.

## Brechas restantes — caça a falhas de 09/09

Revisão por leitura do código atual + `pnpm test` 43/43 (14 index + 29 regressions), `lint`, `fmt:check`, `cargo fmt --check` e `clippy --all-targets --offline` verdes. Nenhum M01–M10 voltou a quebrar no caminho feliz. Abaixo, o que restava fora dele, por severidade. `[INFERENCE]` onde a confirmação exige broker/carga. Correções desta revisão marcadas `[x]` com a solução. Revisão das RFCs em `docs/rfc/implemented`: contrato de entrega no cancel (0001), isolamento nativo + recuperação confirmada (0006), rejeição de modo misto testada (0004), rejeição-com-assinatura (0007) e fallback de zero (0010). Revisão de follow-up: cancel é interrupção documentada (RFC-0001); drenagem vale para disconnect, com 2 regressões. Follow-up da RFC de drenagem (`implemented/0011`): graça com `warn(dropped=N)` verificado por subscriber de captura (6/6 nativo), lifecycle real 12/12 e manual-commit 9/9 com broker local.

### Alta — perda de dados/metadados silenciosa

- [x] **P2 — `Pending` vaza em flush com erro.** Corrigido: ambas as coletas removem do snapshot as entradas ainda `Pending` quando o flush falha (tiveram uma janela cheia sem confirmar); callbacks tardios caem no descarte `Vacant` documentado. Ver `flush_delivery_results` / `with_filter` em [kafka_producer.rs](src/kafka/producer/kafka_producer.rs).
- [x] **P3 — Flush manual global rouba `Delivered` de `send` concorrente.** Corrigido em duas camadas: coleta por `Entry` (só `Delivered` é removido, `Pending` fica) + `Vacant` vira erro explícito de interferência em vez de `Ok` curto; wrapper JS serializa `send`/`flush` por instância ([kafka-client.ts](js-src/kafka-client.ts), `createProducer`). Uso nativo-direto concorrente segue desencorajado.
- [x] **P4 — `last_delivery_results` global com overwrite.** Mitigado: serialização no wrapper elimina a corrida para usuários da API pública (documentado em `createProducer`); nativo-direto concorrente mantém o slot compartilhado como limitação documentada. Getter global preservado por compatibilidade.
- [x] **P1 — `isTombstone: true + payload` descarta bytes sem erro.** Corrigido: `send_single_message` rejeita com `InvalidArg`; seed de M05 corrigido para tombstone válido; nova regressão `P1` ([regressions.test.ts](js-tests/unit/regressions.test.ts)); regra documentada em `MessageProducer` ([model.rs](src/kafka/producer/model.rs)).
- [x] **H1 — `create_topic` fatal aborta o `subscribe` inteiro.** Corrigido: criação tentada para todos os tópicos, assinatura prossegue, erro agregado (`Failed to create topic(s) before subscription: <tópico>: …`) retornado ao final com o consumer assinado. M07 endurecido para o nome do tópico.
- [x] **H2 — `try_send` com mpsc cheio descarta lote parcial sem erro.** Corrigido: tentativa limitada com graça de um batch-timeout e `warn` com a contagem quando descarta, nas streams batch e compacta.
- [x] **H3 — Lista mista vira `assign` exclusivo.** Corrigido: listas mistas rejeitadas com `InvalidArg`; semântica documentada em `subscribe`.
- [x] **A2 (harness) — `receiveWithTimeout` perde mensagem.** Corrigido: leituras por `recvBatch(1, timeout)` com deadline nativa; rebalance agora rejeita em 10 s. Ver [consumer-manual-commit.test.mjs](js-tests/integration/consumer-manual-commit.test.mjs).
- [x] **A3 (contrato) — `SendFailure` por regex + `any`-cast.** Mitigado: formato extraído para `partial_send_error_message` com palavra exata travada por asserção no teste M06 parcial; corrida do getter eliminada pela serialização. O `any`-cast permanece como fronteira napi inevitável.
- [x] Serial descarta prefetch no cancel, batch entrega. Contrato fechado como interrupção (WHATWG): cancel abandona a coleta em-voo e pode descartar o parcial; itens já armazenados por coletas anteriores seguem emitidos; disconnect drena via tentativa limitada (2 regressões de drenagem no disconnect).

### Média — contrato/teste

- [x] Serial descarta prefetch no cancel, batch entrega. Corrigido: serial agora armazena o lote e drena antes de fechar — cancel nunca descarta dado consumido.
- [x] `pending_error` + `Mutex::lock().unwrap()` em future async. Corrigido o panic-via-FFI: 7 sítios usam `unwrap_or_else(|e| e.into_inner())`. As 3 visibilidades (imediato em `recv`/`recv_batch`, terminal nas streams) foram mantidas por forma de chamada e documentadas como contrato.
- [x] `flush` manual nunca inspeciona `item.error`. Corrigido: inspeciona e lança `Message delivery failed` como o caminho filtrado; metadados ficam em `getLastDeliveryResults()`.
- [x] `expandCompactBatch` sem validação. Corrigido: `assertCompactBatchArrays`/`assertSameLength` nos 3 caminhos + teste de batch ragged em [index.test.ts](js-tests/unit/index.test.ts).
- [x] Cancel `flatten(void)` vs `expand(await)`; `HWM = Math.max`. Corrigido: `flatten` aguarda teardown; HWM explícito respeitado (`??= 16`) + teste no path batch (`batchSize: 100`, HWM 4).
- [x] Falso-verde M02/M07/M10/M06. Corrigido: matriz M02 (avançada isolada, simplificada isolada), M07 exige o nome do tópico, M10 trava o clamp de `0`, M06 parcial trava o prefixo exato da mensagem.
- [x] Integração `recv()` nu + rebalance warn-and-continue. Corrigido no arquivo (harness) e job `integration-kafka` adicionado ao CI (apache/kafka KRaft); sem validação em CI real nesta revisão.
- [ ] `commit Async` só existe como evento broadcast buffer 100 ([context.rs](src/kafka/consumer/context.rs), ~90–115); sem listener o erro some. Documentar ou retornar erro no modo Async com listener ausente.
- [x] `fetchMetadataTimeout: 0 → 2000` clamp + TPL vazio. Corrigido/documentado: regra no campo `fetch_metadata_timeout` ([model.rs](src/kafka/consumer/model.rs)) + teste de `0`; `subscribe([])` e TPL sem partições rejeitados com `InvalidArg`. As 3 leituras de metadata bloqueantes por tópico permanecem (natureza librdkafka; extrair para `spawn_blocking` é refactor futuro).

### Baixa — sem ação imediata

- `objectMode` só checa flags, nunca fluxo; flush-localhost em `index.test.ts` não testa nada; `pause/resume` pré-assignment no-op; normalizações divergentes só em log nativo; `nanoid(5)` latente (contextos isolados por instância); headers/key verificados OK. (`O(n²)` no `unregister` eliminado via corte posicional.)

## Testes e qualidade — TODOs

### Correções já observadas

- [x] Remover o `catch` que escondia falhas do teste básico de commit manual.
- [x] Exigir uma mensagem válida antes de executar o commit nesse teste.
- [x] Remover a dependência de ordem de iteração do `HashMap` em `headers_test`, por inspeção de [kafka_util.rs](src/kafka/kafka_util.rs). `cargo test` não linka neste crate (`cdylib`, símbolos napi ausentes no macOS) — verificado idêntico no baseline; cobertura Rust via testes JS contra o binding compilado.
- [x] Incluir `js-tests/**` nos overrides de lint e alinhar a configuração do oxlint. O lint passou.
- [x] Adicionar testes unitários de rejeição de `objectMode: false` e expansão de tombstones nos formatos compactos.

### Pendências de regressão e integração

- [x] **Substituir a verificação superficial de auto-commit.** Coberta em [regressions.test.ts](js-tests/unit/regressions.test.ts) com verificação de valores efetivos e precedência de `enableAutoCommit` vs `enable.auto.commit`.
- [x] Cobrir a precedência de auto-commit com opções ausentes, configuração avançada isolada, opção simplificada isolada e valores conflitantes.
- [x] **Adicionar deadline real ao teste de commit manual.** `maxPolls = 20` substituído por deadline de tempo (`deadline = Date.now() + 20_000`), `receiveWithTimeout` e cleanup em `finally`. Ver [consumer-manual-commit.test.mjs](js-tests/integration/consumer-manual-commit.test.mjs), linhas 375–391.
- [x] Incorporar ao repositório a regressão de `streamOptions: { highWaterMark: 4 }`.
- [x] Incorporar a regressão de atribuição de múltiplos tópicos e partições em lista única.
- [x] Ampliar tombstones para APIs de consumo nativo direto, compacto e instrumentação de diagnóstico.
- [x] Encerrar consumers, readers e streams criados pelos testes, inclusive quando uma asserção falhar (blocos `finally`).
- [x] Executar integração com Kafka no [CI do pacote](../../.github/workflows/CI.yml): job `integration-kafka` (apache/kafka KRaft + `KAFKA_BROKERS`, roda `consumer-manual-commit.test.mjs`). Adicionado, sem rodada real no GitHub nesta revisão.
- [ ] Validar offsets e retomada após reinício com broker real.

## Melhorias de arquitetura, documentação e desempenho — TODOs

Estes itens complementam as correções funcionais; não foram classificados como bugs reproduzidos.

- [ ] **Dimensionar buffers também em bytes.** O limite de quatro batches restringe a quantidade de batches, mas o consumo de memória depende do tamanho das mensagens, do batch e dos demais buffers.
- [ ] Documentar a relação entre prefetch, tamanho de batch, buffers nativos, cancelamento e `highWaterMark`.
- [x] Documentar o contrato de `isTombstone` e sua interação com payload ausente, payload preenchido e flag explícita `false`; avaliar validação de combinações contraditórias. Resolvido com validação: `true + payload` rejeitado (`InvalidArg`), regra no tipo `MessageProducer`, regressão `P1`.
- [x] Documentar a precedência de auto-commit e o comportamento de atribuição manual em listas mistas. Listas mistas agora rejeitadas; semântica em `subscribe`.
- [ ] Atualizar instruções de desenvolvimento que ainda mencionam `build:debug`, dprint e `__test__/integration/compose.yaml`, alinhando-as aos scripts atuais e a `js-tests/integration/docker-compose.yml`.
- [x] Adicionar verificação de logs com credenciais fictícias para prevenir regressão de M03 (incorporado em [regressions.test.ts](js-tests/unit/regressions.test.ts)).
- [ ] Separar codec de batches compactos, coleta e ciclo de vida do consumer em módulos menores após estabilizar os contratos.
- [x] Eliminar as fontes de perda silenciosa mapeadas em "Brechas restantes" (P2/P3/P4/H2, drenagem no disconnect): expirar `Pending`, delimitar flush manual, resultados por lote, erro explícito em `try_send Full`.
- [ ] Medir throughput, latência p95/p99, RSS, memória externa e estado após cleanup antes de afirmar ganhos de desempenho.
- [ ] **Avaliar `tokio::sync::oneshot` por mensagem para as confirmações do producer (M06).** Criar um protótipo com o `Sender` no `DeliveryOpaque`, preservando os contratos de timeout, callbacks tardios, falhas parciais e `autoFlush: false`, inclusive com envios e flush concorrentes. O [benchmark isolado](benchmarks/delivery-tracking/ANALISE.md), com 180 medições e pelo menos quatro milhões de mensagens por processo, apresentou throughput mediano de 1,34× a 3,08× o de um único `DashMap` para batches de 256 sem expiração; com oito produtores, o p99 do batch caiu de 229 µs para 71 µs. Esses resultados medem registro e coleta, sem Kafka, NAPI ou espera assíncrona por mensagem. **Critério de conclusão:** validar correção e medir throughput, p95/p99 e memória com Kafka real, incluindo espera dos receivers no Tokio, backlog e concorrência; adotar a mudança somente se os ganhos se mantiverem sem regressão dos contratos. Ver [metodologia e reprodução](benchmarks/delivery-tracking/README.md) e [resultados completos](benchmarks/delivery-tracking/RESULTS.md).
- [x] Eliminar as fontes de perda silenciosa mapeadas em "Brechas restantes" (P2/P3/P4/H2, serial-vs-batch no cancel): expirar `Pending`, delimitar flush manual, resultados por lote, erro explícito em `try_send Full`.
- [x] Tornar `create_topic` e `assign` explícitos por tópico (H1/H3): sem abortar a assinatura inteira nem converter subscribe em assignment estático silencioso.
- [x] Substituir o acoplamento JS↔string do Rust por erro estruturado (A3) e validar `expandCompactBatch`/HWM/cancel nos dois modos de stream.

## Ordem recomendada para as brechas restantes

1. Expirar/evitar `Pending` órfão (P2) e delimitar o flush manual por escopo (P3); resultados por lote em vez de getter global (P4).
2. Rejeitar `tombstone + payload` (P1) e inspecionar `item.error` no flush manual (P5 divergente).
3. `create_topic` por tópico sem abortar a assinatura (H1); `try_send Full` com erro explícito (H2); rejeitar ou documentar listas mistas (H3).
4. Erro estruturado do Rust, fim da regex/`any` (A3); `recv` com deadline real no harness (A2); validar lengths no `expandCompactBatch`; uniformizar cancel e respeitar HWM.
5. Job de integração com Kafka no CI; endurecer M02/M07/M10; documentar `commit Async`, `fetchMetadataTimeout 0` e TPL vazio.
6. Medir desempenho e separar responsabilidades internas.

Itens 1–4 e o job de CI do item 5 concluídos nesta revisão (38/38 unit); restam `commit Async` sem listener, `spawn_blocking` nas leituras de metadata e o item 6.

Cada correção deve incluir seus testes de regressão. As verificações pendentes permanecem abertas mesmo quando a implementação do problema original já está marcada como concluída.

## Verificações executadas em 09/09

| Verificação                                                   | Resultado                                                                   |
| ------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `pnpm test`                                                   | 43 testes passaram (14 index + 29 regressions)                              |
| `pnpm exec tsc --noEmit`                                      | Passou                                                                      |
| `pnpm lint`                                                   | Passou (0 warnings, 0 errors)                                               |
| `pnpm fmt:check`                                              | Passou                                                                      |
| `cargo clippy --all-targets --offline`                        | Passou (0 warnings, 0 errors)                                               |
| `cargo fmt --check`                                           | Passou                                                                      |
| Atribuição de dois tópicos com offsets explícitos             | Ambos permaneceram atribuídos                                               |
| `highWaterMark` personalizado                                 | Modo de objetos preservado                                                  |
| Tombstone, buffer vazio e conteúdo normal com mock librdkafka | Distinção preservada nos caminhos nativos direto e compacto                 |
| Cancelamento de stream em tópico vazio com mock               | Fluxo cancelado interrompe consumo de mensagens posteriores                 |
| Cancelamento de stream serial e com fila de prefetch          | Stream interrompe sem deadlock ou consumo adicional                         |
| Criação de tópico com broker inacessível                      | Falha fatal propagada por `subscribe()`                                     |
| Timeout de envio e callback posterior                         | Envio rejeita; callback tardio descartado atomicamente sem vazamento        |
| Flush concorrente a múltiplos `send()`                        | Apenas o lote fotografado é descartado; envios concorrentes são confirmados |
| Falha parcial de enqueue com `autoFlush: true`                | Mensagens aceitas são confirmadas e recuperáveis via `SendFailureError`     |
| Preservação de erro em leitura de batch e stream compacta     | Mensagem válida é entregue; EOF de Phase B é lançado na leitura seguinte    |
| Tombstone contraditório (`isTombstone` + payload)             | Rejeitado com `InvalidArg`; regressão `P1`                                  |
| Lista mista (manual + subscribe)                              | Rejeitada com `InvalidArg`                                                  |
| Drenagem no disconnect (batch compacto e serial)              | Parcial coletado alcança leitor vivo após `disconnect()`                    |
| `highWaterMark` explícito no path batch                       | Respeitado (`batchSize: 100`, HWM 4 preservado)                             |
| Falha de criação com nome do tópico                           | Erro agregado cita `unreachable-create-topic`                               |
| `cargo test` (Rust)                                           | Não linka neste crate `cdylib` (igual no baseline); cobertura via JS        |
| Drenagem nativa (`native-drain`)                              | 6/6: resume, bloco preservado e término com `warn(dropped = 32)` verificado |
| Lifecycle com Kafka real                                      | 12/12 (slow/backlog/cancel/prefetch/partial × serial/batch/compact)         |
| Commit manual com Kafka real                                  | 9/9 após trocar espera de rebalance (inexistente sob assign) por assignment |
