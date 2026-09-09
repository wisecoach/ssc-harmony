# HANDOFF-20260908: unfinished 卡死根因定位 —— 热 key 上的「固定优先级 reservation 持锁 + hold-and-wait」真死锁

> 承接 HANDOFF-20260907-dsn-unfinished-drain-analysis.md §3.7 的“下一聚焦”：
> 那永不释放的 holder 是谁 / 为何不动。本轮不做新实验，直接对 234148 长窗口
> （log 下载 2026-09-08 02:41~02:45，结果 030312/031330 = unfinished 4711 平台期）
> 在 `ssc-validator-*.log` 里对 representative unfinished 逐笔逐 shard 追踪。
> **结论：4711 不是可自愈的活锁，而是热 key 上「固定优先级(不可变 nonce) reservation
> 持锁 + 跨分片 hold-and-wait」造成的真 liveness 死锁——force-admit / CMH 都救不了。**
> 复现/证据/建议修复方向见下。

## 0. 一句话结论（回应用户“彻底卡死，不是活锁”）
unfinished 主体是一批**同写一个热分片(shard1)热 key 的跨分片交易**。它们形成一条
**按提交时固定优先级(nonce)排序的无限 hold-and-wait 链**：
```
~967k 次 reservation-skip 的候选(如 0xe838…) 等 K_de3d
   ← 持 K_de3d 者 TX_A(0x106935…) 又等 K_f541
        ← 持 K_f541 者 TX_F(0x62a2298d…) 又等下一个更高优先 key…
```
链上每个成员都是「在某热 key / 某 shard 已能 lock，却在另一 shard(如 shard2)或另一 key
上永远被更高优先挡死」的 partial winner。因为：
1. 优先级 = nonce（提交时定死，永不老化/降级）；
2. retryScheduler admission 每块**贪心地按这个固定优先级先到先得地 reserve key**；
3. 被选中(高优先)的持锁者若自己无法推进（缺别的 shard/key 的 leg），它**仍每块抢占着热 key
   预约却永不释放/永不终局**；
4. 反饥饿 force-admit 只把“饿着的候选”临时放行（>=50 块一次），**不剥夺/不放逐持锁者**，
   下块持锁者又抢回 → 无净进展。
净效果：一整组热 key 交易 3h 内零终局（rollback=0、close=0、永远 `set signal`/reservation-skip），
**不是“每轮在推进只是慢”的活锁，而是无人能前进的真死锁**（用户判断成立）。

## 1. 本轮证据链（2026-09-08 长窗口 log，rate=200/shard4/vpn4）
log: `~/go/src/github.com/harmony-one/logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate=200_vpn=4/`
（ssc-validator-*.log，网络 23:35 → 02:45，~3h10min；结果 031330：unfinished=4711）

### 1.1 representative 卡死交易
- e838…（shard1, cross=1, csv sim=0）：9040 里 6071 行，全是
  `reservation skipped candidate (key already reserved by higher-prio)`
  落在 K_de3d，skipBlocks 17→22，每 ~2s 一次，到 02:42:57 仍无终局。
- TX_A = 0x106935…（shard1 origin, 跨 s0/s1/s2）：
  - shard1(9040)：12163 行 = 5777 `set signal` + 5554 reservation-skip（等 K_f541）+ 138 `cannot wound`；
    持 K_de3d 去 wound 别人（40 次），自身 111 次 force-admit、retry-commit failed 105；
    3h 内 locked:true 仅 37 次（且越到后期越稀，最后 02:36），**永不 close/commit/rollback**。
  - shard2(9080)：6104 行 = 5582 reservation-skip + 101 `wounded by higher priority`，
    **locked:true = 0**（这条 leg 永远上不了锁 → CXT 凑不齐 commit 票）。
  - shard0(9000)：372 行 = 104 `retry commit locked:true` + 4 commit vote + CommitSimulation
    （这条 leg 是好的、愿意投票）。
  ⇒ partial winner：s0 已就绪，s1/s2 的热 key 腿永远凑不齐 → 整单永不 commit，又因锁着 s1 的
    K_de3d 而挡死后面 ~967k 次尝试。
- TX_F = 0x62a2298d…（K_f541 的现持有者，跨 s1/s2）：9040 6599 行 + 9080 6099 行，
  同样 5554 reservation-skip + 111 force-admit + 107 retry-commit failed，3h 无终局。
  ⇒ 它持 K_f541 挡 TX_A，自己又等更高优先的另一个 key/另一 shard → 链条上顶，全卡。

### 1.2 热 key 规模
- K_de3d（0x4030A3D…:0xde3d…）：9040 单 log 3h 内 967,332 次 reservation-skip、1,582 `cannot wound`、
  328 wound、362 genuinely-uncovered。K_f541 类似（224,021 次提到、490 cannot wound）。
  ⇒ 是极热 key（大量跨分片交易同写同一合约/存储槽），不是普通偶发冲突。

## 2. 为什么这是“真死锁”而非可自愈活锁（机制层面）
对照代码 `ssc/retry_scheduler.go` OnBlockCommitted reservation：
- 候选按 `isChainReadyLocal` 优先、再按 `nonce` 升序排序（固定全序，priority 提交时定死）。
- 依次 reserve：低优先遇到已 reserve 的冲突 key → `reservation skipped`，累计 skip>=50 块就
  `force-admit` 一次（`reservationAntiStarvationBlocks = 50`）。
- **force-admit 只是把饿着的候选也加进 selectedTx，并不撤下/回滚已 reserve 的更高优先持锁者。**
  于是：持锁者若自己无法推进（缺跨 shard leg / 缺另一 key），它会**每块都以高优先身份抢占同一热 key
  预约、却永不真正 lock/终局**；被放行的候选下一块又被它抢回。`CanLock`/`CheckLock` 仍失败 → 永不 Ready。
- 反饥饿理论上每 ~100s 触发一次（111 次/3h），但因“持锁者不动”而无效；CMH 只认链上锁环
  （global∪pending），这些边大多在 TLV/reservation 调度层 + 跨分片缺腿，waitEdge 不覆盖
  （rollback 全程仅 225，几乎不挑它们当 victim）→ 无任何逃生机制 → **真死锁**。

## 3. 修复方向（供下轮设计，未实现）
1. **持锁者 yield / 老化，而不是只喂饿者**：对“reservation 连续持锁 N 块却未能 lock/推进”的
   当前持锁者做**让位（视为被 wound / 回池 / 或本块不抢该 key）**，把热 key 让给仍能推进的下一优先，
   而不是把饿者硬塞上去。可配合“持锁块数 / 无进展块数”作为本地、确定性的让位条件。
2. **force-admit 真正剥夺冲突**：放行饿者时，对造成冲突的**当前持锁者撤回其该 key 的 reservation**
   （若持锁者只在 TLV 层、未上链锁），而非叠加上去。使“每块至少真正推进一个”。
3. **跨分片 partial winner 有界逃生（呼应 §3.6/3.7 的 DAG-stuck 归零方向）**：某 CXT 已在若干
   related shard 就绪/上锁、却因某条腿 N 块仍无法就绪时，**整单有界回滚（含已就绪腿、放锁）**，
   把“永久占热 key 的僵尸 partial winner”转成“有界 commit 或整单 rollback”，从根上不再制造
   hold-and-wait 链顶。此点与之前 DAG 撤保护（无效，§3.7）不同：目标是**整单逃生**，不是逐腿撤本地保护。
4. （更深）热 key 工作负载层面：0x4030A3D… 这类被数千交易同写的存储槽是放大源；但按 §3.1 教训
   勿把“慢因”当“根因”——调度层的持锁不释放才是机制。

## 4. 建议下一步
- 先按方向 1+2 出最小 DSN（reservation 持锁让位/剥夺），`go build ./ssc/...` 后同步跑一轮
  clean（rate=200），看 4711 是否下降、热 key 链是否被打断；验收以远程长窗口为准。
- 若想先验证“顶层持锁者也永不终局”到哪一层，可继续沿 K_f541 持有者往上追（本轮已证 TX_A/TX_F
  两层都不终局，链条已成立）。
- 计数/新日志一律 Info 级；日志用 ssc_grep.sh。

## 5. 本轮未做 / 边界
- 未跑新实验（只读现有 234148/031330 log）；未改任何代码。
- 未把 4711 逐一归因到“reservation 链”还是“ready-quorum/DAG-stuck”；本结论聚焦
  88% 的 non-DAG 主体（§3.5）——即 reservation hold-and-wait；DAG-stuck(ready-quorum) 另见 §3.6。
