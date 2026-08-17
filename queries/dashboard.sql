-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
-- hive_partitioning: PARTITION_BY gỡ cột event_date ra khỏi file, giá trị chỉ
-- còn nằm trong tên thư mục `event_date=2026-08-09/`. Bật cờ này để DuckDB đọc
-- lại cột đó từ đường dẫn — và cũng chính nhờ đường dẫn mà nó loại được 13/14
-- thư mục TRƯỚC khi mở bất kỳ file nào.
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
where customer_name = 'ACME'
  -- Cột đứng MỘT MÌNH một vế (sargable). Bản cũ viết
  -- `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` — cột bị bọc trong một
  -- lời gọi hàm, nên engine không so được kết quả hàm với tên thư mục
  -- partition, cũng không so được với min/max của row group: buộc phải mở
  -- toàn bộ 5.000 file rồi mới biết file nào có ích.
  and event_date = '2026-08-09'
group by 1
