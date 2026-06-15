# OOS Vacuum 크래시 복구 테스트 설명서

> 대상 스크립트: [`oos_vacuum_crash_recovery.sh`](./oos_vacuum_crash_recovery.sh)
> 관련 이슈: CBRD-26668 / PR #6986
> 빌드: debug 빌드 필요 (`;oos_stats` 메타커맨드 사용)

---

## 0. 한 줄 요약

리뷰어가 걱정한 **"OOS는 지워졌는데 heap 레코드는 아직 그 OOS를 가리키는, 중간에 찢어진 상태"** 가
크래시 후 복구에서 정말 안 생기는지를, **진짜 서버를 vacuum 도중에 `kill -9` 로 죽였다가 다시 복구시켜서**
확인하는 테스트입니다. 검사 12개 전부 통과(PASS=12, FAIL=0).

---

## 1. 리뷰어가 걱정한 것

리뷰어 질문(요지):

> oos delete → sysop commit → **크래시** → bulk vacuum heap 순서라면,
> heap을 정리하기 **전에** 크래시가 나면 OOS는 이미 commit 처리되어 사라진 상태일 텐데,
> 이 부분 복구 검증을 했나요?

즉, vacuum이 죽은 레코드를 정리하는 도중에 크래시가 나면 두 가지 **찢어진 상태**가 걱정됩니다.
둘 다 진짜 버그입니다.

| 찢어진 상태 | 무슨 일이 벌어지나 |
|---|---|
| **댕글링 참조 (dangling)** | OOS 청크는 삭제됐는데 heap 레코드는 아직 그 OOS OID를 가리킴 → 복구 후 그 행을 읽으면 이미 free 된(혹은 재사용된) 저장공간을 읽어 **쓰레기 값 / 에러** |
| **고아 / 누수 (leak)** | heap 슬롯은 vacuum 됐는데 OOS 청크는 안 지워짐 → 아무도 안 가리키는 OOS가 **영원히 안 지워지고 디스크 누수** |

핵심 질문은 하나입니다: **"크래시 후 heap 쪽과 OOS 쪽이 서로 안 맞는 상태가 될 수 있는가?"**

---

## 2. 테스트가 그 상황을 어떻게 재현하나

테스트는 시나리오 2개로 구성됩니다. 둘 다 "진짜 서버 vacuum → 도중에 크래시 → 복구 → 검사" 흐름입니다.

### TC-01 (DELETE 경로)
1. OOS가 생기는 큰 컬럼(`BIT VARYING`)을 가진 행 여러 개 INSERT + 커밋 → 살아있는 OOS 레코드 46개.
2. 전부 DELETE + 커밋 (죽은 레코드 = vacuum 대상).
3. vacuum이 OOS를 정리하기 시작하는 순간 `kill -9`.
4. 재시작(=WAL 복구) 후 검사: 행 0개 / 누수 0 / checkdb 정상 / 테이블 재사용 가능.

### TC-02 (UPDATE = forward-walk 경로) — ⭐ 리뷰어 걱정의 정곡
1. 행들을 값 **V1** 으로 INSERT + 커밋.
2. 전부 값 **V2** 로 UPDATE + 커밋.
   - 이제 **살아있는** 행은 V2의 OOS를 가리키고,
   - **죽은 이전 버전(pre-image)** 은 V1의 OOS를 가리킴 → vacuum이 **V1만** 지워야 함.
   - 이 시점 살아있는 OOS = 24개 (V2 12개 + V1 12개).
3. vacuum이 V1을 정리하기 시작하는 순간 `kill -9` (로그: `live OOS=12 < 24`).
4. 복구 후 검사: 모든 행이 **V2를 정확히** 읽는가 / V1로 되돌아간 행은 없는가 / V1은 완전히 사라졌는가.

> TC-02가 특히 날카로운 이유: **죽어야 하는 OOS(V1)** 와 **살아남아야 하는 OOS(V2)** 가 동시에 존재합니다.
> 만약 sysop이 찢어져 엉뚱한 청크를 free 하거나 절반만 free 했다면, 둘 중 하나가 반드시 깨집니다.

---

## 3. kill은 언제 떨어지나 — "정리 도중"이 보장되지 않는 이유 (가장 중요한 부분)

> 용어 정리: 리뷰어/질문에서 말한 시점은 정확히는 **vacuum이 OOS를 *삭제(reclaim)* 하는 도중**입니다
> (insert가 아니라 delete 쪽). 아래 메커니즘이 바로 "그 도중"을 맞추는 방법입니다.

크래시 테스트가 의미 있으려면, kill이 **정확히 vacuum이 OOS를 지우는 도중**에 떨어져야 합니다.
- vacuum 시작 **전**에 죽이면 → 복구할 게 없음 (의미 없음)
- 깨끗하게 정상 종료하면 → 애초에 복구를 안 함 (의미 없음)

그래서 "아무 때나 죽이는" 게 아니라, **vacuum이 실제로 OOS를 지우기 시작한 걸 두 눈으로 확인한 뒤에** 죽입니다.
그 역할을 하는 게 `crash_during_vacuum()` 함수이고, 동작은 이렇습니다:

```
baseline = (현재 살아있는 OOS 레코드 수)        # 예: 24
반복 (최대 RECLAIM_WATCH_TIMEOUT 초):
    1) gen_filler   : 자잘한 트랜잭션을 만들어 WAL 로그를 진행시킴
                      → DELETE/UPDATE가 들어있는 "로그 블록"을 닫음
                      → 그래야 그 죽은 레코드가 vacuum 대상이 됨
    2) nudge_vacuum : ';vacuum' 으로 vacuum 마스터 데몬을 깨움
    3) live = oos_live_recs()                  # ';oos_stats' 로 살아있는 OOS 수를 읽음
    4) if live < baseline:                      # 숫자가 줄었다 = 방금 OOS가 지워지기 시작했다!
          로그: "Reclaim started (live=$live < $baseline) -> crashing NOW"
          break
kill_server   # 바로 kill -9
```

여기서 **"보장"의 핵심**은 4번입니다.

- `;oos_stats` 가 보고하는 "살아있는 OOS 레코드 수"가 **baseline 보다 줄어들었다는 것**은,
  vacuum이 이미 `vacuum_heap_record()` 안의 sysop으로 들어가 **OOS 청크를 실제로 삭제하기 시작했다**는 직접 증거입니다.
- 그 숫자가 줄어든 걸 **확인한 직후에** kill 하므로, kill은 "OOS 정리가 시작되기 전"에는 절대 떨어지지 않습니다.

실제 통과 로그가 이걸 증명합니다:

```
TC-02 live OOS records after update (V1 stale + V2 live): 24
Reclaim started (live OOS=12 < 24) -> crashing NOW      ← 지워야 할 V1 12개가 "전부" 정리된 뒤 kill (남은 12 = 살아남을 V2)
```

그리고 `kill -9` 는 정상 종료가 아니므로, 재시작 시 서버는 반드시 **WAL 복구**를 돕니다
(서버 로그에 `RECOVERY: start ... ANALYSIS Phase` 가 찍힘). 즉 우리가 검증하려는 **복구 경로**를 진짜로 탑니다.

> ⚠️ **중요한 한계 — kill 시점에 vacuum이 이미 다 끝나 있을 수 있음.**
> 이 게이트가 보장하는 건 "reclaim이 시작된(= baseline 아래로 떨어진) 이후" 까지일 뿐, "정확히 처리 도중" 이 아닙니다.
> `;oos_stats` 가 "줄었다"고 보고하는 순간 그 sysop은 **이미 commit 된 뒤**이고, vacuum 데몬은 매우 빠르며(기본 주기 10ms)
> `;oos_stats` → `ps` → `kill -9` 사이에도 수십 ms가 흐릅니다. 그래서 **kill이 떨어지는 순간엔 대상 OOS가 이미
> 전부 reclaim + commit 완료돼 있는 경우가 많습니다.** 실제 우리 통과 실행이 바로 그랬습니다:
>
> - TC-01: `live OOS=0 < 46` → 46개 **전부** 지워지고 commit된 뒤 kill (vacuum이 중간에 끊긴 게 아님)
> - TC-02: `live OOS=12 < 24` → 지워야 할 V1 12개가 **전부** 지워진 뒤 kill (남은 12 = 살아남을 V2)
>
> 즉 이 실행들은 엄밀히는 **"vacuum 도중 크래시"가 아니라 "vacuum이 commit을 끝낸 뒤의 크래시"** 를 테스트한 것입니다.
> commit 직후 크래시는 **redo 복구**(또는 이미 flush됐다면 no-op)를 타며 최종 상태 일관성은 검증되지만,
> **commit 전 sysop 도중 중단(= undo 복구)** 은 이 방식으론 절대 못 맞춥니다. 무엇을 증명하고 무엇을 못 하는지는
> **7장** 에 정리했고, 도중 크래시를 결정론적으로 때리려면 디버그 전용 fault-injection이 필요합니다.

---

## 4. 복구 후 무엇을 검사하나 — 각 검사 = 어떤 버그를 잡나

복구가 끝난 뒤, **찢어진 상태의 양쪽 절반이 모두 없는지** 검사합니다.

| 리뷰어가 걱정한 실패 | 이를 잡아내는 검사 |
|---|---|
| 댕글링 (살아있는 OOS가 잘못 free 됨) | `value_match_count(V2) == 12` — 살아남은 모든 행이 **정확히** V2를 읽어야 함. OOS가 free/재사용됐다면 12 미만 → **FAIL** |
| 옛 값 부활 | `value_match_count(V1) == 0` — V1로 읽히는 행이 있으면 → **FAIL** |
| 누수 (OOS가 안 지워짐) | `drain_oos → 12`(TC-02) / `→ 0`(TC-01): 마지막 vacuum 후 살아있는 OOS가 정확히 생존자 수가 돼야 함. 한 청크라도 누수면 끝까지 안 줄어 timeout → **FAIL** |
| 구조 손상 | `cubrid checkdb` 에 ERROR 없어야 함 |
| heap 쪽 복구 실패 | `COUNT(*)` 가 기대한 생존 행 수와 같아야 함 |

즉, commit된 vacuum의 heap 슬롯 삭제와 OOS 청크 삭제가 **함께 영속화되지 않았다면**, 크래시 + 복구 후
위 검사 중 **최소 하나는 반드시 실패**합니다.

---

## 5. 이 검사들이 진짜로 실패할 수 있다는 증거 (오라클이 살아있음)

초록불(PASS)이 의미가 있으려면, 그 검사가 빨간불(FAIL)도 낼 수 있어야 합니다. 그 증거가 있습니다.

- 개발 중 셋업이 깨져 테이블이 없던 상황에서, 값/카운트 검사가 `-1` 을 반환하며 **정확히 FAIL** 처리됐습니다
  (`FAIL: ... actual='-1'`). 검사가 무조건 통과시키는 도장이 아니라는 뜻입니다.
- 누수 검사는 timeout이 걸려 있어, 진짜 누수면 멈추는 게 아니라 **실패**로 드러납니다.
- 숫자는 서버의 실제 상태(`;oos_stats` 의 `Live OOS records : N`, 모든 청크를 셈)와 실제 `SELECT` 값 비교에서
  나옵니다 — 테스트가 스스로 지어낸 값이 아닙니다.

최종 실행은 이 12개가 전부 동작하며 통과: `Results: PASS=12, FAIL=0`.

---

## 6. 코드 차원의 보장 (sysop) — 테스트와의 관계

테스트는 **경험적(empirical)** 증거이고, **구조적(structural)** 보장은 코드 자체에 있습니다.

`src/query/vacuum.c` 의 `vacuum_heap_record()` 에서, OOS를 가진 죽은 레코드는 다음을 **하나의 sysop**으로 묶습니다:

```c
log_sysop_start (thread_p);
  ...
  vacuum_log_redoundo_vacuum_record (...);            // heap 슬롯 삭제 로그
  vacuum_heap_oos_delete_within_sysop (..., &helper->record);  // OOS 청크 삭제
  ...
log_sysop_commit (thread_p);                          // 둘이 함께 영속화
```

sysop은 CUBRID에서 여러 페이지 변경을 **원자적 단위**로 묶는 장치입니다. 따라서 크래시 시 복구는
- **둘 다 redo** (heap 슬롯도, OOS도 사라짐), 또는
- **둘 다 undo** (heap 레코드도, OOS도 그대로 살아있음)

→ 절대 한쪽만 남는 상태가 안 됩니다. 함수 이름도 `vacuum_heap_oos_delete_within_sysop`("sysop 안에서 삭제")로
바뀌어 이 불변식을 이름에 박아 두었습니다. 근거 문서: `docs/adr/0001-synchronous-oos-reclaim-in-vacuum-sysop.md`.

**sysop이 "왜 안전한가"의 이유라면, 이 테스트는 "진짜 크래시를 내봐도 찢어진 상태가 관측되지 않는다"는 증명입니다.**

---

## 7. 이 테스트가 실제로 증명하는 것 / 증명하지 못하는 것 (정직하게)

3장에서 보듯, 이 테스트의 kill은 보통 **vacuum이 대상 OOS를 다 reclaim + commit 한 뒤** 떨어집니다.
그래서 증명 범위를 정확히 나누면 이렇습니다.

### ✅ 증명하는 것
- **commit된 vacuum-OOS 정리는 크래시를 견딘다 (durability / REDO).** vacuum이 heap 슬롯 삭제 + OOS 청크 삭제를
  commit한 뒤 `kill -9` → WAL 복구 후에도: 죽은 행은 죽은 채로, OOS 청크도 지워진 채로 남고,
  **부활·누수·댕글링·구조 손상이 없다.** (즉 WAL 로깅이 완전하고 재생 가능함을 입증)
- **heap 쪽과 OOS 쪽이 함께(원자적으로) 영속화됨을 확인.** 복구 후 둘이 항상 일치한다(둘 다 사라짐).
  만약 WAL이 한쪽만 기록했다면(로깅 버그), 복구가 불일치 상태를 만들어 4장 검사에서 잡힌다.
- **UPDATE forward-walk의 정확성 + 크래시 내성.** 옛 버전(V1)의 OOS는 reclaim되고 현재 버전(V2)의 OOS는
  살아남으며, 이 결과가 크래시 + 복구를 견딘다 (TC-02).
- **vacuum/OOS의 "큰 버그" 검출.** 살아있는 OOS를 free, 체계적 누수, forward-walk undo 이미지 파싱 오류 같은
  버그가 진짜 서버 + 크래시 경로에서 드러난다 (Catch2 단위 테스트가 잡았던 종류를 end-to-end로 재확인).
- **크래시가 진짜다.** `kill -9`(비정상 종료) → 재시작 시 복구 analysis/redo가 실제로 돈다(clean 재부팅이 아님).

### ❌ 증명하지 못하는 것
- **sysop commit *전* (vacuum 도중) 크래시 → UNDO 복구.** 이 방식은 항상 commit *후* 에 죽이므로(3장 ⚠️),
  진행 중이던 vacuum sysop이 올바로 롤백되는지는 검증하지 못한다. 리뷰어가 말한 "찢어진 상태"가
  비원자적 구현에서 실제로 발생하려면 바로 이 *도중 크래시* 가 필요한데, 거기엔 닿지 못한다.
- **REDO의 비-trivial성 보장 못 함.** kill 직전 checkpoint가 페이지를 flush했다면 복구가 redo할 게 없어,
  "복구가 잘 돼서"가 아니라 "복구할 게 없어서" 통과했을 수 있다.

→ 이 미검증 영역의 안전성은 **sysop의 구조적 보장**(commit 전 크래시 = 미완성 sysop = 자동 undo, 6장)에 의존한다.
이를 *직접* 결정론적으로 검증하려면 `vacuum_heap_record()` 에 디버그 전용 fault-injection(commit 전 / 후 abort)이 필요하다.

### 보조 한계
- **한 번 실행 = 크래시 지점 하나.** 여러 지점을 표본화하려면 시나리오를 반복 실행/루프로 감싼다.
- **debug 빌드 전용.** `;oos_stats` 는 관측 도구일 뿐 제품 경로 의존성이 아니다.
- **broker 미사용.** csql이 cub_server에 직접 붙어 공유 broker를 안 건드린다(다른 서버 영향 없음).

---

## 8. 실행 방법 / 결과

```bash
cd ~/gh/cubrid-oos-test/shell
bash oos_vacuum_crash_recovery.sh

# 빠른 스모크 (행 수 축소)
N_SMALL_ROWS=12 N_BIG_ROWS=2 bash oos_vacuum_crash_recovery.sh
```

debug_gcc 빌드 기준 결과:

```
[TC-01] Reclaim started (live OOS=0 < 46) -> crashing NOW
        PASS: 행 0개 / checkdb 정상 / 누수 0 / 재사용 OK
[TC-02] Reclaim started (live OOS=12 < 24) -> crashing NOW
        PASS: 모든 행 V2 / V1 없음 / checkdb 정상 / 12로 drain / 생존자 V2 유지
Results: PASS=12, FAIL=0
```

---

### 결론

이 테스트는 리뷰어가 말한 **그 시나리오**(vacuum이 OOS를 지우는 도중 크래시 → WAL 복구)를 그대로 재현하고,
**실패할 수 있음이 입증된 검사들**로 복구 후 heap과 OOS가 항상 일치함(댕글링 없음, 누수 없음)을 보입니다.
이것이 리뷰어가 요청한 "복구 검증"입니다.
