# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lê Nguyễn Phước Thành  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 41.5s
  run 2/3 … 31.2s
  run 3/3 … 31.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau sự cố mạng, người trực bấm Clear Task cho chạy lại; sáng hôm sau `gold_training_set` phình lên, và **mỗi lượt chạy tiếp lại tăng thêm**. Không một dòng lỗi nào, `dbt test` vẫn 9/9 pass. Khoanh vùng bằng hai truy vấn: nguồn `silver_tickets` giữ đúng **12.480 hàng / 12.480 ticket** (1 hàng/1 ticket, không lặp), nhưng đích thì có ticket xuất hiện tới **5 lần**. Input sạch mà output bẩn ⇒ lỗi nằm ở **cách model được materialize**, không nằm ở dữ liệu. |
| **Nguyên nhân** | Model khai `materialized='incremental'` nhưng **không khai `unique_key`**. dbt là một bộ sinh mã: macro `get_incremental_default_sql` rẽ nhánh trên `unique_key`, và khi thiếu khoá nó rơi vào nhánh sinh ra `insert into … select … from tmp` (đọc được trong `.venv/…/dbt/include/global_project/…/incremental/strategies.sql`). Đây không phải một lựa chọn tuỳ hứng mà là **tất yếu logic**: muốn sinh `MERGE … ON dest.? = src.?` hay `DELETE … WHERE ? IN (…)` thì dbt phải điền được một *tên cột* vào chỗ `?`, mà nguồn văn bản duy nhất cho chỗ đó là `unique_key`. Không khai thì câu lệnh hợp lệ duy nhất còn dựng được là `INSERT` thuần — và `INSERT … SELECT` **không bao giờ đọc bảng đích**, nên nó không có khả năng thay thế, chỉ có khả năng đắp thêm. Nói gọn: một bảng có grain **entity** bị cấu hình như một bảng **log**, khiến phép ghi mất tính idempotent. Từ đó **mọi cơ chế phục hồi ở tầng điều phối — Clear Task, `retries: 2`, backfill của scheduler — đều biến thành cơ chế nhân bản**, vì tất cả chúng được thiết kế trên giả định ngầm rằng tác vụ bên dưới an toàn khi chạy lại. Lỗi im lặng vì `append` là hành vi hoàn toàn chính đáng với bảng log, nên dbt không có cách nào phân biệt "người dùng quên khai khoá" với "người dùng cố ý muốn append". |
| **Vì sao `delete+insert` theo partition ngày không cứu được** | Nguồn CDC có `op='u'`. `silver_tickets` là `table` dựng lại toàn bộ mỗi lượt và chỉ giữ bản ghi mới nhất, nên `_ingested_at` của một ticket **di cư** sang partition mới mỗi lần ticket được cập nhật. Ở lượt chạy 08-09, dòng cũ cần dọn đang nằm ở partition **08-05**, trong khi lượt chạy hôm nay chỉ được phép động vào partition **08-09**. Một phép xoá bị đóng khung trong phạm vi partition không bao giờ với tới dòng đã chuyển nhà. Kết luận tổng quát: **phép thay thế phải giới hạn bởi *khoá*, không bởi *ngày*.** |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Giữ nguyên mệnh đề `WHERE` theo `run_date` — nó là tối ưu phạm vi quét cho backfill, không phải cơ chế đảm bảo đúng đắn.<br>`dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. Đây là **lớp phòng thủ ngoài**, không phải nguyên nhân gốc: chúng chỉ hạ số lần lỗi bị kích hoạt (từ 14 lần backfill đồng thời xuống 1), chứ một cú Clear Task duy nhất vẫn đủ làm trùng nếu model chưa sửa. |
| **Bằng chứng** | trước: **38.750** hàng (trạng thái ban đầu, thừa 26.270) · sau: **12.480** hàng, `1 hàng / 1 ticket ✓ không lặp` · checksum 3 lượt giống hệt: `8dd7c98653` |

**Về hình dạng tăng trưởng.** Bảng **không** phình theo một hệ số nhân đều. Mệnh đề `WHERE` lọc theo `_ingested_at` của **một** `run_date`, nên mỗi lượt chạy chỉ nhân bản phần ticket rơi vào partition ngày đó — tăng trưởng là **cộng dồn theo partition**, không phải nhân đều toàn bảng. Con số thừa 26.270 phân rã đúng theo cấu trúc này: `26.270 = 12.480 × 2 + 1.310`, trong đó **1.310** chính là số bản ghi `op='u'` trong nguồn CDC — đúng những ticket được cập nhật, tức những ticket **đi qua mệnh đề `WHERE` ở hai partition ngày khác nhau**. Chính hiện tượng di cư partition này (xem ô dưới) là thứ khiến `delete+insert` theo ngày không cứu được.

Trên warehouse của tôi sau vài lượt chạy thêm, con số đã lên **51.230** và tiếp tục tăng — đó là bằng chứng trực tiếp cho tính không idempotent: không có điểm dừng, mỗi lần chạy lại là một lần đắp thêm.

**Bằng chứng mạnh nhất — SQL mà dbt thực sự gửi xuống database** (`dbt/target/run/lab17/models/gold/gold_training_set.sql`):

```sql
-- TRƯỚC: không đọc bảng đích, nên không thể thay thế
insert into "warehouse"."main"."gold_training_set" (...)
    ( select ... from "gold_training_set__dbt_tmp20260817084030048134" )

-- SAU: đối chiếu theo khoá, xuyên qua mọi partition
MERGE INTO ... AS DBT_INTERNAL_DEST USING ... AS DBT_INTERNAL_SOURCE
    ON (DBT_INTERNAL_SOURCE.ticket_id = DBT_INTERNAL_DEST.ticket_id)
WHEN MATCHED     THEN UPDATE BY NAME
WHEN NOT MATCHED THEN INSERT BY NAME
```

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu ~5% so với đối chiếu thủ công (8.645 / 9.100), và chỉ thiếu ở những ngày **đã chạy xong từ lâu**; ngày mới thì đủ. Đáng chú ý: bảng vẫn `ỔN ĐỊNH ✓` — chạy lại bao nhiêu lần cũng ra đúng 8.645. Nó **ổn định mà vẫn sai**, khác hẳn nhiệm vụ 1. |
| **P99 độ trễ đo được** | **2,726 ngày** (≈ 2 ngày 17 giờ) *(p50 = 0,128 · p95 = 1,814 · max = 2,945 · tỷ lệ tới muộn hơn 1 ngày = 5,05%)* |
| **Lookback đã chọn** | **3 ngày** — vì `event_date` là kiểu `DATE` nên lookback phải là số ngày nguyên, buộc **làm tròn lên**: `ceil(2,726) = 3`. Hai nguồn độc lập xác nhận cùng con số: độ trễ tính theo ngày nguyên lớn nhất trong dữ liệu là **3 ngày**, và cặp `(ngày, khách)` khó nhất cũng cần đúng **3 ngày** lookback. |
| **Nguyên nhân** | Điều kiện lọc incremental là `where event_date > (select max(event_date) from {{ this }})`. Biểu thức `max(event_date)` là một **watermark**, và nó có hai tính chất giết chết dữ liệu về muộn: nó **chỉ tiến về phía trước**, và nó được tính từ `event_date` của dữ liệu **đã xử lý** chứ không phải từ thời điểm dữ liệu **tới kho**. Một event xảy ra 08-12 nhưng tới kho 08-15 sẽ gặp watermark đang ở 08-14 ngay lần đầu nó xuất hiện: `08-12 > 08-14` sai, nên bị bỏ qua. Hôm sau watermark còn cao hơn. **Bản ghi về muộn, theo định nghĩa, luôn nằm phía sau watermark — nó lỡ chuyến tàu đúng một lần rồi vĩnh viễn không có chuyến thứ hai.** Đây là lỗi *thiếu sót* chứ không phải lỗi *lặp*: dữ liệu không hề được xử lý lần nào, nên kết quả sai một cách hoàn toàn tái lập — và vì thế không có bất kỳ tín hiệu bất ổn nào để cảnh báo. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện lọc thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day` — `>` thành `>=` để không bỏ sót chính ngày biên, cộng lookback 3 ngày. **Đồng thời** thêm `unique_key = ['event_date','customer_id']` + `incremental_strategy = 'merge'`: nới window khiến cùng một cặp được tính lại ở nhiều lượt, nếu model chỉ biết `insert` thì kết quả cộng dồn — tức tái tạo đúng lỗi nhiệm vụ 1 trên bảng khác. Grain gồm hai cột nên khoá là một list hai phần tử. |
| **Bằng chứng** | trước: **8.645** hàng · sau: **9.100** hàng (= 14 ngày × 650 khách, lưới đầy đủ) · checksum 3 lượt giống hệt: `3db448685c` |

**Bằng chứng định lượng cho root cause.** Với mỗi cặp `(event_date, customer_id)`, đo độ trễ của event **sớm nhất** thuộc cặp đó:

| Event sớm nhất trễ | Số cặp | |
|---:|---:|---|
| 0 ngày | **8.645** | ← có ít nhất một event tới đúng hạn |
| 1 ngày | 64 | |
| 2 ngày | 389 | |
| 3 ngày | 2 | |
| **Tổng** | **9.100** | |

Dòng đầu **đúng bằng** số hàng của bảng khi còn lỗi. Bảng cũ chứa chính xác những cặp có ít nhất một event tới trong ngày; cặp nào mà **toàn bộ** event đều tới muộn thì không bao giờ lọt qua watermark. 64 + 389 + 2 = **455** — khớp tuyệt đối với con số thiếu hụt. Không còn là "khoảng 5%" mà là một con số giải thích được tới từng đơn vị.

**Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?**

> `max` được quyết định bởi **đúng một bản ghi**, nên nó không phải một thống kê ổn định: một record dị thường duy nhất kéo nó đi bao xa cũng được, và lần đo tháng sau có thể ra con số hoàn toàn khác. Xây một tham số hệ thống trên đại lượng không tái lập được là tự chuốc lấy rủi ro. P99 thì ổn định qua các lần lấy mẫu và phát biểu được thành cam kết với đội hạ nguồn: "99% dữ liệu tới trong vòng N ngày".
>
> Về chi phí, hai lựa chọn không đối xứng. **Mỗi ngày lookback thêm là một ngày event phải gộp lại từ đầu, ở mọi lượt chạy, mãi mãi** — đó là chi phí định kỳ chứ không phải chi phí một lần. Nới window để cứu 1% cuối nghĩa là trả giá vĩnh viễn cho một trường hợp cá biệt. Cách làm đúng là lookback theo P99 cho đường chạy hàng ngày, còn phần đuôi hiếm hoi thì xử lý bằng một lượt full-refresh định kỳ (ví dụ hàng tuần) — tách chi phí hiếm ra khỏi đường chạy nóng. *(Đây là đề xuất thiết kế; tôi chưa hiện thực lượt full-refresh đó trong `dags/`, vì ở bộ dữ liệu này lookback 3 ngày đã phủ hết 9.100 cặp.)*
>
> Ở bộ dữ liệu này hai giá trị tình cờ cùng làm tròn lên thành 3 ngày (P99 = 2,726 · max = 2,945), nên lựa chọn không tốn thêm gì. Nhưng căn cứ được ghi nhận là **P99**, vì đó mới là đại lượng còn đúng khi dữ liệu đổi.
>
> **Ghi nhận một điểm yếu còn lại:** độ trễ theo ngày lịch phân bố `0 → 108.862 · 1 → 14.165 · 2 → 3.842 · 3 → 2.593`, tức lag lớn nhất **đúng bằng 3 ngày**. Lookback 3 phủ vừa khít 100%, **không dư một ngày biên an toàn nào**. Nếu phân bố trễ dịch sang 4 ngày, bảng sẽ lại thiếu hàng — và thiếu **âm thầm**, vẫn `ỔN ĐỊNH ✓`, đúng loại lỗi mà nhiệm vụ này đang dạy. Trong hệ thống thật, đây là lý do phải **giám sát chính phân bố độ trễ** như một metric, chứ không chốt cứng một hằng số rồi quên.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Team backend đổi kiểu cột `priority` từ số sang chuỗi hôm 08-10. Pipeline **không hề dừng**, `dbt test` vẫn 9/9 pass, số hàng vẫn đúng — nhưng model phân loại từ hôm đó dự đoán kém hẳn. Soi `silver_tickets` thấy hai bất thường **ngược chiều nhau**: tỷ lệ `NULL` rất lớn, đồng thời xuất hiện `0`, `5`, `-1` trong khi contract quy định `priority ∈ 1..4` — tổng cộng **6.606 / 12.480 hàng Silver** sai (hơn một nửa bảng). Đây là hỏng **âm thầm** — tín hiệu duy nhất nằm ở chất lượng model tận cuối chuỗi. |
| **Nguyên nhân** | Macro chuẩn hoá dùng `try_cast(priority_raw as integer)`, tức nó hỏi *"chuỗi này có ép được về số nguyên không?"* trong khi contract thật sự là *"đây có phải một mức ưu tiên hợp lệ trong 1..4 không?"*. **Kiểm tra sai câu hỏi, nên sai theo hai hướng ngược nhau cùng lúc**: quá **chặt** với nhãn chữ (`urgent`, `high`, `medium`, `low` ép không được → `NULL`, mất trắng dữ liệu tốt kể từ 08-10), và quá **lỏng** với số ngoài miền (`0`, `5`, `-1` ép được → nhận, dù vô nghĩa). Gốc rễ khái niệm: **hợp lệ về kiểu ≠ hợp lệ về ngữ nghĩa** — `-1` là một `integer` hoàn hảo và vẫn là một mức ưu tiên vô nghĩa. Việc backend đổi nhãn là **schema evolution**: nguồn đổi *cách biểu diễn*, ý nghĩa không đổi; phản ứng đúng là **hấp thụ** (dịch về 1..4) chứ không phải từ chối. Coi schema evolution là dữ liệu hỏng đồng nghĩa với vứt bỏ dữ liệu tốt chỉ vì bên gửi đổi cách viết. |
| **Ba nhóm giá trị và cách xử lý** | Đo được **16 giá trị phân biệt**, không có khoảng trắng thừa, nhãn chữ toàn viết thường:<br>**Nhóm 1 — số hợp lệ** `1` `2` `3` `4` (6.846 bản ghi): đúng contract ban đầu → **giữ nguyên**.<br>**Nhóm 2 — nhãn chữ** `urgent` `high` `medium` `low` (7.142 bản ghi): schema evolution, có tài liệu API làm căn cứ → **map** về 1, 2, 3, 4.<br>**Nhóm 3 — hỏng thật** `P1` `P2` `unknown` `0` `5` `-1` `''` `NULL` (**312** bản ghi): → **quarantine**.<br>*(Ba con số này đếm theo **bản ghi CDC**: 6.846 + 7.142 + 312 = 14.300 = toàn bộ `bronze_tickets_cdc`. Khác đơn vị với con số 6.606 ở ô Triệu chứng, vốn đếm theo **hàng Silver**.)*<br>Ranh giới nhóm 2 / nhóm 3 là *có tài liệu làm căn cứ hay phải suy diễn*. `P1` trông rất giống "priority 1", nhưng nó không nằm trong tài liệu API — đoán ý người gửi là cách tạo ra lỗi im lặng, nên đưa vào quarantine để người trực xác minh với backend. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` ba nhóm, trả `NULL` cho nhóm 3. Macro được **cả hai** model dùng nên chúng là một tập và phần bù của nó, không thể lệch nhau.<br>`dbt/models/silver/silver_tickets.sql`: chèn CTE `valid` lọc bản ghi hỏng **trước** `row_number()`.<br>`dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null`.<br>`dbt/models/silver/schema.yml`: `contract.enforced: true`, thêm `not_null` + `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng grain: 1 hàng / 1 bản ghi CDC), checksum 3 lượt giống hệt `ebb89036fb` · `dbt test` **11/11 pass** (bản gốc 9) · `silver_tickets.priority` sạch, không NULL, luôn ∈ 1..4 · `silver_tickets` giữ đủ **12.480** ticket · `gold_training_set` không đổi (12.480) |

**Chi tiết đáng lưu ý — thứ tự lọc quyết định số hàng của bảng.** File gốc xếp hạng (`row_number`) trước rồi mới chọn `_rn = 1`. Nếu chỉ thêm điều kiện lọc vào cuối, bản ghi hỏng vẫn **thắng cuộc xếp hạng** (nó mới nhất), chiếm mất vị trí `_rn = 1` rồi bị loại — kéo theo cả ticket. Mô phỏng cả hai thứ tự trên `bronze_tickets_cdc`:

| Thứ tự | Số ticket trong Silver |
|---|---:|
| Lọc trước → xếp hạng sau (đúng) | **12.480** ✓ |
| Xếp hạng trước → lọc sau (sai) | 12.168 ✗ |

Nguyên tắc tổng quát: **phải xác định tập ứng viên hợp lệ trước, rồi mới chọn người thắng trong tập đó.** Ta loại *bản ghi* hỏng, không loại cả *ticket* — ticket đó rơi về trạng thái hợp lệ gần nhất của chính nó. (Đo thêm: cả 312 bản ghi hỏng đều là `op='u'`, không có tombstone `op='d'` nào hỏng, nên việc lọc trước không làm ticket đã xoá sống lại.)

**Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng khi gặp bản ghi lỗi?**

> **Chặn ở Silver, không chặn ở Bronze.** Bronze phải là bản ghi trung thực về những gì nguồn đã gửi — nó không phán xét, mọi luật lệ áp ở Silver. Nếu Bronze từ chối bản ghi lỗi, ta mất ba thứ: (1) *bằng chứng điều tra* — khi sự cố xảy ra, câu hỏi đầu tiên luôn là "nguồn thật sự gửi gì?", không có dữ liệu thô thì không trả lời được; (2) *khả năng xử lý lại* — và bài này là minh chứng đắt giá: nếu Bronze chặn "priority không phải số" từ 08-10, **toàn bộ 7.142 bản ghi schema evolution đã bị huỷ vĩnh viễn**, không còn gì để chạy lại sau khi ta phát hiện luật của mình sai; (3) *khả năng đo lường* — không lưu thì không đếm được số bản ghi lỗi, không thấy được xu hướng. Nguyên tắc: **luật lọc là thứ có thể sai, dữ liệu thô thì không — đừng để một luật có thể sai phá huỷ thứ không thể tạo lại.**
>
> **Không để `dbt test` fail và dừng DAG**, vì cân theo quy mô: **312 bản ghi lỗi so với 12.480 ticket + 130.683 event + 31.200 chunk hoàn toàn bình thường** đang chờ được phục vụ. Để 0,2% dữ liệu chặn đứng tất cả phần còn lại là để cái đuôi vẫy con chó — đội CSKH mất dashboard, RAG index không cập nhật, agent định tuyến chạy trên dữ liệu cũ. Quan trọng hơn, bản ghi lỗi **không bị bỏ qua**: chúng vào `quarantine_tickets` kèm `reject_reason` phân loại theo bốn kiểu lỗi, tức một hàng đợi đếm được, theo dõi được, đặt ngưỡng cảnh báo được — bảng quarantine *chính là* cơ chế cảnh báo. Chặn cứng còn tạo động cơ xấu: pipeline chết lúc 2h sáng vì vài bản ghi lẻ, vài lần là có người tắt test đi, và lúc đó ta mất luôn cả cảnh báo lẫn dữ liệu.
>
> Nhưng không phải lỗi nào cũng nên cho qua. Nếu 90% bản ghi hỏng thì đó không còn là nhiễu, đó là nguồn sập — và lúc ấy dừng lại mới đúng. Cảnh báo nên đặt theo **ngưỡng tỷ lệ**, không theo **có/không**.

---

## 4 · Bài mở rộng (EXTRA.md)

| | |
|---|---|
| **Bài đã làm** | **A và B** (cả hai) |

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard CSKH mất 38 giây mới load, ba tháng trước chỉ 2 giây, không ai sửa dòng code nào. Đo lại: **5.000.000 rows scanned** cho một truy vấn chỉ trả về 1 hàng, trên **5.000 file** Parquet chứa tổng cộng 130.683 hàng thật. |
| **Nguyên nhân** | Hai lỗi cộng hưởng, cùng một gốc: **engine chỉ bỏ qua được file mà nó biết là vô ích *trước khi* mở file, và thông tin duy nhất nó có trước đó là đường dẫn.** (1) Dataset là 5.000 file phẳng tên `part-00000.parquet`… — đường dẫn **không mang bất kỳ thông tin filter nào**, nên engine buộc phải mở toàn bộ 5.000 file rồi mới biết file nào có ích; và vì `rows scanned` được làm tròn lên theo từng file, 130.683 hàng thật biến thành 5.000.000 đơn vị công quét (38×). (2) Điều kiện ngày viết là `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` — cột bị **bọc trong một lời gọi hàm**, nên optimizer không so được kết quả hàm với tên thư mục partition, cũng không so được với min/max statistics của row group; predicate mất tính sargable và không đẩy xuống được lớp lưu trữ. Số hàng tăng dần theo thời gian, còn số file tăng theo, nên hiệu năng suy giảm mà không ai "sửa gì cả". |
| **Cách khắc phục** | `tools/compact.py`: ghi lại dataset thành `data/gold_events_v2` với `partition_by (event_date)` — 14 giá trị → 14 thư mục, loại ngay 13/14 (không chọn `customer_name` vì 650 giá trị sẽ tái tạo đúng small-file problem đang phải chữa); `order by event_date, customer_name, event_time` với ý đồ gom hàng cùng khách nằm liền nhau cho min/max của row group hẹp lại; `row_group_size 2048` vì 130.683/14 ≈ 9.334 hàng/ngày nên mặc định 122.880 sẽ gói trọn một ngày vào **một** row group. Hai quyết định sau là lựa chọn **theo nguyên tắc** — đo lại thì chúng không đóng góp gì vào con số 536× (xem ghi chú bên dưới). Kèm `assert` số hàng cũ = mới, và xoá thư mục đích trước khi ghi để chính `make compact` cũng idempotent.<br>`queries/dashboard.sql`: đọc qua `hive_partitioning = true` và viết lại điều kiện thành `event_date = '2026-08-09'` — cột đứng một mình một vế. |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324** (giảm **536,3×**, yêu cầu ≥10×) · `files` **5.000 → 14** · `result hash` **`4379e4c5d9f3` không đổi** · thời gian 17.456 ms → **93 ms** · kết quả truy vấn giống hệt: `('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)` |

> **Thứ tự chạy bắt buộc:** `make seed-extra` → `make compact` → `make explain`. `queries/dashboard.sql` đã trỏ vào `data/gold_events_v2/`, nên bỏ qua bước `make compact` sẽ khiến truy vấn không tìm thấy file. (`tools/compact.py` xoá thư mục đích trước khi ghi nên chạy lại bao nhiêu lần cũng cho cùng kết quả.)

**Output `make explain` sau khi sửa:**

```
  queries/dashboard.sql
  --------------------------------------------------------------
                             TRƯỚC        HIỆN TẠI      MỤC TIÊU
  rows scanned           5,000,000           9,324     ≤ 500,000   ✓
  rows on disk             130,683         130,683   (tham khảo)
  files                      5,000              14        ít hơn   ✓
  result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
  thời gian (ms)                 —            93.0   (tham khảo)

  => giảm 536.3× (cần ≥ 10×)

  kết quả truy vấn (1 hàng):
    ('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)
```

> **Ghi chú trung thực — công lao thuộc về đâu.** Tôi đo thử với `row_group_size` = 122.880 / 4.096 / 2.048 và **cả ba đều cho cùng `rows scanned` = 9.324**, tức metric `OPERATOR_ROWS_SCANNED` của DuckDB 1.5.5 đếm theo *file được mở* chứ không phản ánh row-group pruning. Vậy trong ba quyết định của `compact.py`, **chỉ `partition_by (event_date)` đóng góp vào con số 536×**; `order by` và `row_group_size` là lựa chọn đúng *theo nguyên tắc* nhưng không được bằng chứng nào trong bài này chứng minh, và tôi cũng chưa đo IO thực tế để khẳng định chúng có ích. Ghi lại ở đây thay vì nhận công.
>
> Về thời gian: triệu chứng nêu 38 giây còn baseline đo trên máy tôi là 17,5 giây. EXTRA.md nói rõ bài chấm theo `rows scanned` chứ không theo thời gian, vì thời gian phụ thuộc cấu hình máy và cache của OS — nên con số thời gian ở đây chỉ để tham khảo.

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test` giết consumer ở giữa lô thứ 7 rồi khởi động lại: bảng **mất** bản ghi. |
| **Nguyên nhân** | Thứ tự thao tác là `commit() → crash → write_batch()`, tức **at-most-once**: offset được ghi nhận **trước** khi dữ liệu chạm đĩa. Chết ở khe hở giữa hai bước thì offset đã dịch qua lô 7 nhưng 500 message của lô đó chưa hề được ghi; lần khởi động lại đọc từ *sau* lô 7 nên **500 message mất vĩnh viễn, và không tầng nào phát hiện được**. Điểm cốt lõi: exactly-once không tồn tại ở tầng giao vận — thứ duy nhất chọn được là **mất dữ liệu** hay **trùng dữ liệu**. Mất là không thể phục hồi; trùng thì phục hồi được ở tầng ghi. Vậy phải chọn at-least-once, và trả giá bằng việc làm phép ghi idempotent. |
| **Cách khắc phục** | `ingest/consumer.py` — hai thay đổi, thiếu một là chưa đủ: (a) đảo thành `write_batch() → crash → commit()` để chuyển sang **at-least-once**; (b) làm phép ghi **idempotent**: thêm `primary key` cho `event_id` trong `DDL` (DuckDB chỉ chấp nhận `ON CONFLICT` khi có ràng buộc duy nhất) và đổi `INSERT` thuần thành `insert … on conflict (event_id) do update set …`. |
| **Bằng chứng** | A (chạy thẳng): 20.000 hàng / 20.000 `event_id`. B: chết ở lô 7, offset commit dừng ở **3.000**. C (khởi động lại): ghi **17.000** message nhưng bảng vẫn đúng **20.000 hàng / 20.000 `event_id`** — 500 hàng được ghi hai lần và `ON CONFLICT` đã hấp thụ chúng. `lost = 0` · `dup = 0` · `C == A` → **BÀI MỞ RỘNG B: ĐẠT ✓** |

**Output `make crash-test`:**

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

Đọc kỹ ba con số này là thấy trọn cơ chế: lô 7 ghi xong **3.500** message nhưng offset chỉ commit tới **3.000** — cửa sổ 500 message nằm giữa "đã ghi" và "đã commit". Restart đọc lại từ 3.000 nên ghi **17.000** (= 20.000 − 3.000), tức 500 message được ghi **lần thứ hai**. Bảng vẫn đúng 20.000 hàng: đó chính là chỗ phép ghi idempotent trả công. Với `INSERT` thuần, con số này đã là 20.500.

> **`DO UPDATE` khác `DO NOTHING` ở đâu khi message được phát lại với nội dung ĐÃ ĐỔI?** Trong kịch bản crash-test, message được phát lại y hệt nên hai lựa chọn cho cùng kết quả. Khác biệt lộ ra khi nguồn sửa bản ghi rồi gửi lại cùng khoá: `DO NOTHING` giữ mãi bản **nhìn thấy đầu tiên** — bảng đóng băng ở trạng thái cũ và bản đính chính bị bỏ qua **trong im lặng**; `DO UPDATE` hội tụ về bản **mới nhất** (last-write-wins). Tôi chọn `DO UPDATE` vì bảng này mang ngữ nghĩa "trạng thái mới nhất của mỗi `event_id`", và vì nó đúng trong cả hai trường hợp: với event thật sự bất biến thì hai lựa chọn cho kết quả như nhau. Nếu bảng mang ngữ nghĩa *append-only log bất biến* thì `DO NOTHING` mới là lựa chọn đúng (rẻ hơn, không ghi lại hàng).

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline **hai lần liên tiếp** rồi so số hàng và checksum. Idempotency là tính chất phải kiểm bằng thực nghiệm, không suy ra được từ việc đọc code. Cụ thể với dbt: mở `target/run/.../<model>.sql` xem câu lệnh **thật** gửi xuống database, đừng tin `config()` sẽ làm đúng điều mình tưởng. |
| 2 | Đối chiếu **hai cột thời gian**: lúc sự việc *xảy ra* (`event_time`) và lúc bản ghi *tới kho* (`_ingested_at`). Đo P99 của hiệu số trước khi tin vào bất kỳ điều kiện lọc incremental nào. Một bảng **ổn định mà vẫn sai** không phát ra tín hiệu gì cả — phải chủ động so với lưới đầy đủ mới thấy. |
| 3 | Nhìn **phân bố giá trị** của các cột quan trọng, không chỉ nhìn số hàng và trạng thái task. Hỏi thêm: các test hiện có đang ràng buộc *kiểu* hay *miền giá trị*? Và khi gặp giá trị lạ, phân biệt cho được **schema evolution** (nguồn đổi cách biểu diễn — phải hấp thụ) với **dữ liệu hỏng** (phải tách riêng), vì xử lý nhầm nhóm thứ nhất sẽ âm thầm vứt đi phần lớn dữ liệu tốt. |
