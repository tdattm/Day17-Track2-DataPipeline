# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Tiến Đạt  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 22.5s
  run 2/3 … 22.2s
  run 3/3 … 22.8s

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
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
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

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng từ 12.480 lên 38.750 hàng sau ba lượt; 12.480 ticket bị lặp. |
| **Nguyên nhân** | Model incremental không khai báo khóa duy nhất nên dbt ghi theo kiểu append/INSERT. Khi retry hoặc chạy lại cùng partition, các dòng ticket đã có bị ghi thêm thay vì được thay thế. Vì nguồn CDC có bản ghi `op='u'`, một entity có thể xuất hiện ở nhiều ngày. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, thêm `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1` để giảm khả năng chạy bù/chạy chồng sau Clear Task. |
| **Bằng chứng** | Trước: 38.750 hàng, 12.480 ticket trùng. Sau: 12.480 hàng = 12.480 ticket phân biệt; checksum ổn định `8dd7c98653deb11388d16eb0342a0fc3`. |

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645/9.100 hàng, thiếu 455 cặp ngày-khách hàng ở các ngày cũ. |
| **P99 độ trễ đo được** | **2,7258 ngày** (~65,4 giờ). Tỷ lệ event tới muộn hơn một ngày là 5,05%; max đo được là 2,9447 ngày. |
| **Lookback đã chọn** | **3 ngày** — làm tròn P99 lên số ngày nguyên. |
| **Nguyên nhân** | Điều kiện incremental `event_date > max(event_date)` chỉ nhận ngày mới hơn ngày lớn nhất đã có. Event xảy ra ở ngày cũ nhưng tới Bronze muộn không thỏa điều kiện ở lượt đến kho và sau đó cũng không còn được quét lại. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`, dùng window `event_date >= max(event_date) - interval 3 day`; thêm `unique_key=['event_date', 'customer_id']` và `incremental_strategy='merge'` để các aggregate tính lại được thay thế, không bị nhân bản. |
| **Bằng chứng** | Trước: 8.645 hàng. Sau: 9.100 hàng, không có khóa ghép trùng; checksum ổn định `3db448685ca59595e9a7dcb883be0e03`. |

Chọn P99 thay vì `max` vì lookback là chi phí phát sinh ở **mọi** lượt chạy sau
đó: mỗi ngày lùi thêm phải đọc và aggregate lại nhiều partition hơn. P99 bảo
vệ mức dịch vụ với phần lớn dữ liệu mà không biến một ngoại lệ hiếm thành chi
phí thường trực. Trong bộ dữ liệu này, max cũng nhỏ hơn 3 ngày nên window đã
bao phủ toàn bộ các độ trễ quan sát được.

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có 6.606 giá trị NULL/ngoài miền 1..4; `quarantine_tickets` rỗng trong khi kỳ vọng 312 bản ghi lỗi. |
| **Nguyên nhân** | Macro cũ chỉ `try_cast` chuỗi sang số: nhãn hợp lệ do schema evolution (`urgent`, `high`, `medium`, `low`) bị đổi thành NULL; đồng thời các số ngoài contract (`0`, `5`, `-1`) lại được chấp nhận. Hơn nữa, nếu lọc lỗi sau khi xếp hạng CDC thì một cập nhật lỗi mới nhất sẽ làm mất cả ticket. |
| **Ba nhóm giá trị `priority` và cách xử lý** | `1..4`: giữ nguyên. `urgent/high/medium/low`: map lần lượt về `1/2/3/4`. `P1`, `P2`, `unknown`, `0`, `5`, `-1`, rỗng, NULL và các giá trị khác: trả NULL và đưa vào quarantine. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql` dùng CASE chung cho Silver và quarantine. `silver_tickets.sql` chuẩn hóa và lọc bản ghi NULL **trước** `row_number()`. `quarantine_tickets.sql` chọn các hàng macro trả NULL. `schema.yml` bật contract và thêm tests `not_null`, `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312, đúng grain 1 hàng/CDC record; không có duplicate CDC record. `silver_tickets` còn đủ 12.480 ticket, priority phân bố: 1=3.134, 2=3.029, 3=3.115, 4=3.202; `dbt test` pass 11/11. |

Nên giữ Bronze ở dạng raw và chặn/tách lỗi ở Silver: Bronze là bằng chứng để
audit, điều tra và reprocess; từ chối ngay ở Bronze sẽ làm mất dấu vết của sự
cố. Không nên dừng cả DAG vì 312 bản ghi lỗi không nên chặn phần dữ liệu hợp
lệ còn lại. Quarantine cho phép pipeline tiếp tục phục vụ và tạo hàng đợi rõ
ràng để đội vận hành xử lý, trong khi contract và tests bảo vệ dữ liệu Silver.

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

Không làm.

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Grain, natural key và SQL dbt thực sự sinh ra khi model incremental retry. |
| 2 | Phân bố chênh lệch giữa thời điểm xảy ra và thời điểm ingest trước khi đặt incremental watermark/lookback. |
| 3 | Phân biệt schema evolution với dữ liệu lỗi thật; đồng thời xác định tầng quarantine và thứ tự lọc/ranking. |
