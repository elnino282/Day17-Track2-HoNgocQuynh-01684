# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Hồ Ngọc Quỳnh  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output nguyên văn của ba lượt chạy</summary>

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 148.4s
run 2/3 … 105.5s
run 3/3 … 100.8s

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
dashboard rows scanned                      ✓ 5,000,000 → 137,662 (36.3×, cần ≥ 10×)
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
| **Triệu chứng** | Mỗi lần retry cùng dữ liệu, `gold_training_set` lại tăng. Trạng thái ban đầu sau ba lượt có 38.750 hàng thay vì 12.480 và cả 12.480 ticket đều bị lặp. |
| **Nguyên nhân** | Model incremental không có `unique_key`, nên dbt thực hiện append thay vì cập nhật. Đây là bảng entity có grain 1 hàng/ticket và nguồn CDC có `op='u'`; vì vậy xóa/chèn theo partition ngày cũng không bảo vệ được một ticket xuất hiện ở nhiều ngày. `catchup=True` và không giới hạn `max_active_runs` còn làm tăng khả năng nhiều run cùng ghi, nhưng không phải nguyên nhân gốc. |
| **Cách khắc phục** | Trong [`gold_training_set.sql`](dbt/models/gold/gold_training_set.sql), đặt `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong [`ai_training_pipeline.py`](dags/ai_training_pipeline.py), đặt `catchup=False`, `max_active_runs=1` để tránh chạy bù và ghi đồng thời. |
| **Bằng chứng** | Trước: **38.750 hàng** · sau: **12.480 hàng**, không lặp `ticket_id` · checksum 3 lượt: **`8dd7c98653` / `8dd7c98653` / `8dd7c98653`** · DAG: **`False / 1`**. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định qua các lần chạy nhưng chỉ có 8.645/9.100 cặp `(event_date, customer_id)`, thiếu 455 hàng ở các ngày cũ. |
| **P99 độ trễ đo được** | **2,7235416667 ngày**. Tham chiếu: P50 = 0,1285763889 ngày; P95 = 1,8011458333 ngày; max = 2,9446875 ngày; 5,003711% event đến muộn hơn một ngày. |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99; với dữ liệu hiện tại, cửa sổ này cũng bao phủ max quan sát được. |
| **Nguyên nhân** | Watermark cũ chỉ nhận `event_date > max(event_date)` ở bảng đích. Ví dụ, event xảy ra ngày 12-08 nhưng tới kho ngày 15-08 có `event_date` nhỏ hơn watermark hiện tại, nên bị bỏ qua vĩnh viễn dù pipeline vẫn chạy thành công. |
| **Cách khắc phục** | Trong [`gold_feature_daily.sql`](dbt/models/gold/gold_feature_daily.sql), tính lại từ `max(event_date) - interval 3 day`, dùng khóa ghép `['event_date', 'customer_id']` và `merge` để lần tính lại thay thế hàng cũ thay vì cộng dồn qua các cửa sổ chồng nhau. |
| **Bằng chứng** | Trước: **8.645 hàng** · sau: **9.100 hàng** · checksum 3 lượt: **`3db448685c` / `3db448685c` / `3db448685c`**. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 là SLA thực dụng: nó bao phủ gần như toàn bộ dữ liệu nhưng giới hạn lượng lịch sử phải quét và tính lại ở mọi lượt. Dùng `max` bao phủ mọi độ trễ đã quan sát, nhưng chỉ một ngoại lệ rất muộn cũng có thể làm cửa sổ và chi phí tái tính tăng lâu dài. Đổi lại, chọn P99 có thể bỏ sót phần đuôi 1%; phần này cần cảnh báo và backfill có chủ đích. Trong bộ dữ liệu hiện tại, max vẫn dưới 3 ngày nên lookback đã chọn không bỏ sót bản ghi nào.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline không dừng nhưng Silver có 6.606 hàng `priority` sai/NULL, trong khi quarantine rỗng; chất lượng dữ liệu huấn luyện vì thế giảm. |
| **Nguyên nhân** | `try_cast` biến các nhãn chữ hợp lệ thành NULL, nhưng lại chấp nhận số ngoài contract như `0`, `5`, `-1`. Ngoài ra, nếu deduplicate trước khi loại bản ghi lỗi, một update lỗi mới nhất sẽ che mất trạng thái hợp lệ trước đó của cả ticket. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `'1'..'4'`: giữ nguyên; (2) `urgent/high/medium/low`: ánh xạ thành `1/2/3/4`; (3) nhãn lạ (`P1`, `P2`, `unknown`), số ngoài miền (`0`, `5`, `-1`), chuỗi rỗng và NULL: trả về NULL để định tuyến vào quarantine. |
| **Cách khắc phục** | Dùng chung macro trong [`normalize_priority.sql`](dbt/macros/normalize_priority.sql) cho Silver và quarantine; chuẩn hóa, loại bản ghi lỗi **trước** `row_number`; bật contract `enforced: true`; thêm test `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** · `silver_tickets` = **12.480 hàng**, priority sạch · **`dbt test` 11/11 pass** · checksum quarantine giữ **`ebb89036fb`** qua ba lượt. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Bronze nên giữ nguyên payload nguồn để điều tra và replay; chuẩn hóa, kiểm tra contract và định tuyến lỗi thuộc Silver. Không nên dừng toàn bộ DAG vì 312 bản ghi lỗi khi hơn 130.000 event và 31.200 chunk hợp lệ vẫn cần được phục vụ. Quarantine cô lập lỗi thành một hàng đợi quan sát được để xử lý riêng, trong khi dữ liệu tốt tiếp tục đi qua pipeline.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **A và B** |
| **Nguyên nhân** | **A:** 5.000 file nhỏ không partition buộc engine mở toàn bộ file; biểu thức `strftime(event_time, ...)` không hỗ trợ partition pruning. **B:** commit offset trước khi ghi tạo at-most-once và làm mất batch khi crash; chỉ đổi thứ tự thành ghi trước/commit sau lại tạo nguy cơ trùng do replay. |
| **Cách khắc phục** | **A:** tạo `event_date`, partition theo 14 ngày, sắp theo `event_date, customer_name`, dùng row group 2.048 và predicate trực tiếp trên `event_date`. **B:** ghi/upsert nguyên tử trước rồi mới commit offset; đặt `event_id` làm primary key và dùng `ON CONFLICT ... DO UPDATE` để replay idempotent và vẫn cập nhật payload mới. |
| **Bằng chứng** | **A:** rows scanned **5.000.000 → 137.662 (36,3×)**; file **5.000 → 14**; rows on disk giữ **130.683**; result hash giữ **`4379e4c5d9f3`**. **B:** crash ở batch 7, offset **3.000**; restart xử lý 17.000 message; kết quả **20.000 hàng / 20.000 event_id**, không mất, không trùng, `C == A`; `BÀI MỞ RỘNG B: ĐẠT ✓`. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain và natural key của từng bảng, rồi kiểm tra retry/backfill có idempotent hay không. |
| 2 | Đo phân bố giữa event time và ingestion time, rồi đối chiếu watermark, lookback và khóa merge. |
| 3 | Phân biệt schema evolution hợp lệ với dữ liệu hỏng; kiểm tra contract, quarantine và thứ tự chuẩn hóa–lọc–deduplicate. |
