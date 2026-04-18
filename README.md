# Binary format of DB

Single file is split into chunks.

**File:**

```
[Meta]
[Chunk]
[Chunk]
[Chunk]
```

**Chunk:**

```
[Meta]
[Row]
[Row]
[Row]
```

**Row:**

```
[Meta]
[Key]
[Value]
```

**Chunk states:**

- NEW
- FILLED

```
AppendRow() {
    GetChunk()
}
```

---

## Нотация псевдокода

| Символ | Смысл |
|--------|--------|
| `snap` | Текущая версия снимка (`getCurrentSnapshotVersion()`). |
| `ver(k)` | Версия записи ключа `k` (для CAS и оптимистических tx). |
| `overlay` | In-memory слой: незакоммиченные изменения текущей сессии / tx. |
| `pageCache` | Кэш недавно поднятых страниц. |
| `→` | Возврат результата; `Ok(v)`, `Err(...)`, `found`. |
| `ABORT` | Откат транзакции с ошибкой конфликта версий. |

Чтение значения ключа везде идёт по одному правилу: сначала `overlay`, затем `pageCache`, затем persistent слой (`mmap` / страницы).

---

## Операции над KV-стором

**Базовые**

| Операция | Смысл |
|----------|--------|
| `GET(k)` | Прочитать значение по ключу. |
| `SET(k, v)` | Записать значение (создать или перезаписать). |
| `DELETE(k)` | Удалить ключ (tombstone в overlay / в логе). |
| `EXISTS(k)` | Есть ли ключ (можно реализовать как лёгкий путь без полного `v`). |
| `RANGE(from, to)` | Итерация по ключам в диапазоне (порядок ключей задаётся схемой хранения). |

**Условные / атомарные**

| Операция | Смысл |
|----------|--------|
| `CAS(k, expected, new)` | Атомарно: если текущее значение `== expected`, записать `new`; иначе ошибка / `false`. |
| `REPLACE(k, v)` | Записать только если ключ уже существует; иначе ошибка (отличается от `SET`). |

**Конкуренция и транзакции**

- Версионирование записей — для атомарного swap и `CAS`.
- Оптимистическое обновление с коротким захватом блокировок на ключи при коммите.
- Транзакция только по заранее объявленному набору ключей: `tx(k1, k2, k3)`.

```
BEGIN(keys)
// любые операции, но только над этими ключами
END
```

---

## Имплементации операций

### GET(k)

```
GET(k) → (value, found bool) | error
    snap = getCurrentSnapshotVersion()
    return findKey(snap, k)

findKey(snap, k):
    if overlay.contains(k):
        return overlay[k]  // изменения текущей итерации / tx + незакоммиченные записи
    if pageCache.contains(k):
        return pageCache[k]
    return mmap.loadPageWithKey(snap, k)  // bloom + бинарный поиск по страницам
```

### SET(k, v)

```
SET(k, v) → error
    // in-memory: обновить overlay и при необходимости ver(k)
    overlay.put(k, v)
    bumpVersion(k)  // для последующих CAS / END(tx)
    optionally append to op-log / flush chunk
```

### DELETE(k)

```
DELETE(k) → error
    if not findKey(snap, k).found:
        return nil  // или ErrNotFound — зафиксировать в API
    overlay.delete(k)  // или tombstone + bumpVersion(k)
    optionally append to op-log
```

### EXISTS(k)

```
EXISTS(k) → (found bool) | error
    (_, found) = GET(k)  // или дешевле: bloom / индекс страницы без загрузки value
    return found
```

### RANGE(from, to)

```
RANGE(from, to) → iterator over (k, v) | error
    // merge: упорядоченный обход persistent слоя + overlay в диапазоне [from, to)
    it = mmap.scanOrdered(snap, from, to)
    mergeWithOverlay(it, overlay, from, to)
```

(Границы диапазона: зафиксировать в API — `[from,to)` или `[from,to]`.)

### REPLACE(k, v)

```
REPLACE(k, v) → error
    (_, found) = GET(k)
    if not found:
        return ErrNotExists
    SET(k, v)
```

### CAS(k, expected, new)

```
CAS(k, expected, new) → (ok bool) | error
    (cur, found) = GET(k)
    if not found && expected != emptySentinel:
        return false, nil
    if cur != expected:
        return false, nil
    SET(k, new)
    return true, nil
```

(Семантика «пустого» ключа и сравнения значений должна совпадать с типом `Value`.)

### BEGIN(keys)

```
BEGIN(keys) → txid | error
    snap = getCurrentSnapshotVersion()
    keyVersions = getKeyVersions(snap, keys)  // снимок ver(k) на старт; иначе ошибка если ключ обязателен
    tx[txid].save(keys, keyVersions)
    return txid
```

### END(txid)

```
END(txid) → error
    for k in tx.keys:
        if k was touched in this tx:
            if ver(k) != tx.savedVer(k):
                ABORT tx
            lock(k)
    for k in tx.keys:
        if k was modified in this tx:
            persist(k)  // overlay → log / страницы
    for k in tx.keys:
        if k was touched:
            unlock(k)
    commit tx
```

---

## Доступные интерфейсы

**KV**

- `get(key)` — overlay, затем кэш страниц, затем mmap.
- `set(key, value)` — in-memory; опционально flush в op-log.
- `delete(key)`
- `exists(key)`
- `range(from, to)` — см. `RANGE` выше.

**MmapedFile**

- Кэш и доступ к страницам на mmap; поиск ключа по bloom + бинарный поиск в странице.

### Oplog

Фасад для записи, сброса на диск и поиска записей с ограничением по версии. Полный пример с интерфейсами и стандартной сборкой зависимостей: [`internal/oplog.go`](internal/oplog.go).

**Абстракции**

| Интерфейс | Роль |
|-----------|------|
| `Oplog` | `Append`, `AcquireNextVersion`, `Flush`, `CommitVersion`, `Find` — публичный контракт для KV. |
| `OplogPersistence` | `AppendRecord`, `Sync` — куда физически пишется лог (файл, mmap, буфер в тестах). |
| `VersionLedger` | `Allocate`, `Commit`, `Abort`, `ActiveReadVersion` — выдача версий транзакциям и видимая для чтений граница. |

**Запись (`LogRecord`)**

- `Key`, `Value` — как в KV; `Value == nil` — tombstone (удаление).
- `Version` — `LogVersion` на момент операции (после `AcquireNextVersion`).

**Сборка по умолчанию**

```go
log := NewOplog(NewMemoryPersistence(), NewLinearVersionLedger())
```

`NewLinearVersionLedger` — упрощённая политика (одна «водная линия» видимости, без дыр от абортов). Для продакшена `VersionLedger` заменяют реализацией с учётом пропусков версий и отложенных коммитов.

**Сокращённый вид контракта**

```go
type Oplog interface {
    Append(rec LogRecord) error
    AcquireNextVersion() LogVersion
    Flush() error
    CommitVersion(v LogVersion) error
    Find(keys []Key, atOrBefore LogVersion) (map[Key]LogRecord, error)
}
```

---

### Черновые заметки по версиям

```
v1
tx1: acquire_tx() -> v2
tx2: acquire_tx() -> v3
tx2: commit_tx(v3) // don't block (flush)
tx1: commit_tx(v2) // rollback (inmemory)

oplog: [v1, (v2), v3]
v2 — no collisions (осталось только сделать запись в лог)
v3 — все операции записаны и все ок
Для чтений — активная версия — v1
Для записи (транзакций) — активная версия v1, после коммита v2 и v3 возможен abort v4, но не v2/v3

v2 — committed -> oplog.version = 3? // надо находить последнюю версию

// таймауты транзакций
// подумать над проблемой потерянного ответа на коммит:
// идемпотентность с т.з. клиента библиотеки + двухфазный клиентский коммит
```

---

MmapFile (working with anonymus pages of the same size without any headers)
- get page
- write to page
- sync page
- free page?
- advice?

ChunkAllocator (working with chunks of different sizes with headers)
- get chunk handle by id
- alloc new chunk handles by size
- free chunk by id

ChunkHandle
- read
- write
- sync
- free
- close
- id

OpLog

Db
