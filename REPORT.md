# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** ____________________  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

## 0 · Kết quả `make verify`

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 147.8s
run 2/3 … 103.6s
run 3/3 … 109.0s

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

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau ba lượt, `gold_training_set` có 38.750 hàng thay vì 12.480; cả 12.480 ticket đều bị lặp và số hàng tiếp tục tăng khi retry. |
| **Nguyên nhân** | Model incremental không khai báo khóa duy nhất nên dbt dùng thao tác chèn thêm. Retry cùng dữ liệu vì thế tạo bản sao thay vì cập nhật. Đây là bảng entity; các bản ghi CDC `op='u'` còn làm cùng ticket được chọn lại ở nhiều ngày vận hành, nên xóa/chèn theo partition ngày cũng không giải quyết đúng grain. |
| **Cách khắc phục** | Trong `gold_training_set.sql`, dùng `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong DAG, tắt `catchup` và đặt `max_active_runs=1` để tránh nhiều run cùng ghi, nhưng đây chỉ là lớp giảm rủi ro; tính idempotent nằm ở model. |
| **Bằng chứng** | 12.480 hàng, không lặp; checksum `8dd7c98653` ở cả ba lượt. DAG được kiểm tra là `False / 1`. |

## 2 · Bảng đặc trưng thiếu hàng ở ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng ổn định nhưng chỉ có 8.645/9.100 cặp `(event_date, customer_id)`; thiếu 455 cặp ở các ngày cũ. |
| **P99 độ trễ đo được** | **2.7235416667 ngày**. P50 = 0.1285763889 ngày, P95 = 1.8011458333 ngày, max = 2.9446875 ngày; 5.003711% event đến muộn hơn một ngày. |
| **Lookback đã chọn** | **3 ngày**, làm tròn lên từ P99; cửa sổ này cũng bao phủ max quan sát được của bộ dữ liệu hiện tại. |
| **Nguyên nhân** | Watermark cũ chỉ nhận `event_date > max(event_date)` của đích. Event xảy ra ngày 08-12 nhưng tới kho ngày 08-15 có ngày sự kiện nhỏ hơn watermark hiện tại, nên bị bỏ qua vĩnh viễn dù pipeline vẫn chạy ổn định. |
| **Cách khắc phục** | Tính lại từ `max(event_date) - interval 3 day`; khai báo khóa ghép `['event_date', 'customer_id']` và dùng `merge` để mỗi cặp được thay thế thay vì cộng dồn qua các cửa sổ chồng nhau. |
| **Bằng chứng** | 9.100/9.100 hàng; checksum `3db448685c` không đổi qua ba lượt, trong khi training vẫn giữ 12.480 hàng. |

P99 được dùng làm SLA thực dụng vì nó giới hạn lượng lịch sử phải quét lại ở mọi lượt. Dùng `max` bao phủ toàn bộ mẫu đã thấy nhưng một ngoại lệ rất muộn có thể làm chi phí tái tính tăng vĩnh viễn. Phần đuôi ngoài cửa sổ nên được giám sát và xử lý bằng backfill có chủ đích; trong bộ dữ liệu này, max vẫn dưới 3 ngày nên cửa sổ đã chọn không bỏ sót bản ghi nào.

## 3 · Kiểu dữ liệu `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline không dừng nhưng Silver có 6.606 hàng priority sai/NULL, còn quarantine rỗng. |
| **Nguyên nhân** | `try_cast` làm mất các nhãn chuỗi hợp lệ (`urgent/high/medium/low`) thành NULL, đồng thời lại chấp nhận các số ngoài contract như 0, 5, -1. Ngoài ra, nếu xếp hạng CDC trước khi loại bản ghi lỗi, một update lỗi mới nhất sẽ làm mất cả ticket dù ticket còn trạng thái hợp lệ cũ. |
| **Ba nhóm và xử lý** | `'1'..'4'` giữ nguyên; `urgent/high/medium/low` ánh xạ thành `1/2/3/4`; `P1`, `P2`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng và NULL được đưa vào quarantine. |
| **Cách khắc phục** | Dùng một macro CASE chung cho Silver và quarantine; chuẩn hóa rồi lọc bản ghi lỗi **trước** khi `row_number`; bật contract `enforced: true`; thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets = 312`, `silver_tickets = 12.480`, priority sạch, `dbt test 11/11 pass`; checksum quarantine `ebb89036fb` ở cả ba lượt. |

Bronze nên giữ nguyên payload nguồn để còn dữ liệu điều tra và replay. Việc chuẩn hóa, kiểm tra contract và định tuyến lỗi thuộc Silver. Không nên dừng toàn bộ DAG vì 312 bản ghi lỗi khi hơn 130.000 event và 31.200 chunk hợp lệ vẫn cần được phục vụ; quarantine biến lỗi thành một hàng đợi quan sát được để xử lý riêng.

## 4 · Bài mở rộng A — Tối ưu query dashboard

| | |
|---|---|
| **Triệu chứng** | Dashboard lọc một khách hàng trong một ngày nhưng phải mở 5.000 file; 130.683 hàng thật bị tính thành 5.000.000 `rows scanned`. |
| **Nguyên nhân** | Các file nhỏ không partition nên path không mang thông tin của `event_date`; engine phải mở toàn bộ file. Predicate `strftime(event_time, ...)` bọc cột trong hàm nên không thể dùng partition pruning hay min/max statistics. |
| **Cách khắc phục** | `tools/compact.py` tạo `event_date`, partition theo ngày (14 giá trị), sắp `event_date, customer_name`, dùng row group 2.048. `queries/dashboard.sql` đọc glob đệ quy với `hive_partitioning=true` và lọc sargable `event_date = date '2026-08-09'`. |
| **Bằng chứng** | `rows scanned`: **5.000.000 → 137.662 (giảm 36,3×)**; file: **5.000 → 14**; rows on disk giữ **130.683**; result hash giữ nguyên **`4379e4c5d9f3`**. |

## 5 · Bài mở rộng B — Consumer bị crash giữa batch

| | |
|---|---|
| **Triệu chứng** | Thứ tự cũ commit offset trước khi ghi. Nếu chết tại `maybe_crash`, offset đã tiến nhưng batch chưa ghi; restart bỏ qua batch đó, tức at-most-once và mất 500 message. |
| **Nguyên nhân** | Offset của transport và transaction của DuckDB là hai trạng thái commit độc lập; không có exactly-once tự nhiên giữa chúng. Chỉ đảo sang ghi trước/commit sau tạo at-least-once: crash làm batch được đọc lại, và `INSERT` thuần khi đó sẽ sinh bản ghi trùng. |
| **Cách khắc phục** | Ghi/upsert nguyên tử trước, gọi `maybe_crash`, rồi mới commit offset. Đặt `event_id` làm primary key và `ON CONFLICT ... DO UPDATE`; replay cùng key không tăng số hàng. `DO UPDATE` cập nhật payload nếu message replay đã đổi, còn `DO NOTHING` sẽ giữ payload cũ. Batch được bind qua JSON để tránh chi phí thực thi từng message. |
| **Bằng chứng** | Crash ở batch 7 sau khi offset mới commit **3.000**; restart xử lý 17.000 message (gồm batch replay) nhưng kết quả vẫn **20.000 hàng / 20.000 event_id**, không mất, không trùng, `C == A`; `BÀI MỞ RỘNG B: ĐẠT ✓`. |

## 6 · Tổng kết

### Đối chiếu trực tiếp với rubric

| Hạng mục | Bằng chứng trong bài | Điểm tự chấm |
|---|---|---:|
| A · Ổn định | Cả 4 checksum giữ nguyên qua 3 lượt chạy | 30/30 |
| B · Tính đúng | Gold lần lượt 12.480, 9.100, 31.200 hàng; training không lặp `ticket_id` | 30/30 |
| C · Chất lượng dữ liệu | Contract enforced; 11/11 test; quarantine 312; priority sạch; Silver đủ 12.480 ticket | 20/20 |
| D · Báo cáo nguyên nhân | Ba nhiệm vụ đều nêu cơ chế gây lỗi; P99 = 2.7235416667 ngày | 20/20 |
| Thưởng A | Scan giảm 36,3×, hash không đổi, số file giảm | +5 |
| Thưởng B | Crash-test đạt; giải thích at-most-once, at-least-once và idempotent write | +5 |
| **Tổng kỹ thuật** | **4/4 tiêu chí chính và 2/2 bài mở rộng đạt** | **110/100** |

Kiểm tra an toàn trước khi nộp: `git diff --numstat -- expected seed/generate.py tools/verify.py tools/explain.py tools/common.py` không trả về khác biệt nội dung; `git diff --check` không phát hiện lỗi whitespace. Vì vậy không vi phạm nhóm file bị cấm sửa trong rubric.

| Nhiệm vụ | Kiểm tra đầu tiên khi tiếp nhận hệ thống |
|---|---|
| 1 | Xác định grain/natural key và kiểm tra retry có idempotent hay không. |
| 2 | Đo phân bố event-time so với ingestion-time rồi kiểm tra watermark/lookback. |
| 3 | Phân biệt schema evolution hợp lệ với dữ liệu hỏng, đồng thời kiểm tra thứ tự lọc và deduplicate. |

Hai bài mở rộng đều đã hoàn thành và vượt tiêu chí tự động; tổng điểm kỹ thuật theo rubric là **110/100** (điểm quy đổi tối đa 100).
