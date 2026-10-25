# Nguồn dữ liệu và giả định

## Provenance

- Repository: https://github.com/KietLe2504/Project_DeepLearning_2025_1
- Commit: `8474917c5c62c1bb9c8acc3f5543b56e94f72604`
- File nguồn: `oil_data.csv`
- File local: `data/raw/oil_data.csv`, sao chép nguyên trạng.
- SHA-256: `3a89eea98b15340be4146a275e24e5683c93ec9bcd63d43ae96fbcdec115add2`
- 5.757 dòng, 10 cột, 2003-01-02 đến 2025-11-26; ngày đã tăng dần, không trùng.

Không sao chép notebook, API key hoặc mô hình của dự án nguồn. Source snapshot được giữ cố định để tái lập thí nghiệm; không tự gọi API cập nhật dữ liệu khi train.

## Profile của snapshot

| Cột | Số giá trị thiếu |
|---|---:|
| cushing_crude_oil_price | 0 |
| dow_jones_adj_close_price | 5 |
| nasdaq_adj_close_price | 5 |
| sp_adj_close_price | 5 |
| usd_to_uk_exchange_rate | 56 |
| federal_funds_rate | 43 |
| bank_prime_loan_rate | 36 |
| treasury_1_year_rate | 44 |
| breakeven_inflation_5_year_rate | 44 |

Giá dầu nhỏ nhất: khoảng -37.63; lớn nhất: khoảng 145.29. Có một quan sát giá không dương. Không loại quan sát này: loại nó vừa thay đổi bài toán vừa che mất giai đoạn thị trường quan trọng.

Tên `adj_close_price` là tên trong dữ liệu nguồn; pipeline chưa xác minh đây là adjusted close hay close. Tên tỷ giá cũng được giữ nguyên, không diễn giải lại chiều quy đổi. `cushing_crude_oil_price` có code thu thập tham chiếu `CL=F`; cần kiểm chứng thêm cách ghép/roll hợp đồng trước khi dùng cho nghiên cứu tài chính sâu hơn.

## Vì sao không dùng compiled_dataset.csv mặc định?

Snapshot compiled có 5.609 dòng và 19 cột; giá dầu và một số cột đã đổi thang. So sánh 5.582 ngày trùng nhau có giá dương cho thấy target compiled bằng log tự nhiên của target raw. Tuy nhiên ngày 2020-04-20 có giá -37.63 trong raw nhưng compiled lưu khoảng 2.90526; không có đầy đủ mã preprocessing giải thích cách xử lý này.

Đảo `exp()` một cách máy móc không phục hồi được lịch sử giá âm. Trộn hai file cũng có thể trộn biến raw với biến log và tạo metric sai đơn vị. Vì vậy pipeline hiện chỉ nhận schema raw 10 cột. Muốn thêm Henry Hub hoặc bộ compiled cần một bước chuẩn hóa nguồn có tài liệu rõ ràng và version mới của data contract.

## Thời điểm thông tin có sẵn

Dữ liệu chỉ có ngày quan sát, không có `published_at`, revision/vintage hoặc múi giờ công bố. Lag 1 phiên cho covariate là giả định thận trọng, **không chứng minh dữ liệu point-in-time chính xác**. Lãi suất/chỉ số kinh tế có thể được công bố chậm hoặc điều chỉnh về sau. Backtest hiện tại chưa loại được hoàn toàn loại rò rỉ này.

Forward-fill chỉ dùng các dòng quá khứ, tối đa 5 phiên; không dùng interpolate hai chiều. Khi vượt giới hạn, feature thiếu được đánh dấu và điền bằng median học từ khoảng train (train+validation khi refit cuối).

## Diễn giải kết quả

- RMSE/MAE phản ánh sai số giá ở thang gốc; không phải phần trăm.
- R² có thể âm; không phải accuracy phân loại.
- `rmse_skill_vs_persistence = 1 - RMSE_model / RMSE_persistence`: dương là tốt hơn baseline, âm là kém hơn.
- Hệ số Linear Regression và feature importance của tree là mô tả mô hình, không phải bằng chứng tác động nhân quả. Các features tương quan mạnh có thể phân chia importance theo nhiều cách.
- Một split duy nhất không đủ để kết luận model ổn định ở mọi chế độ thị trường. Không chọn lại hyperparameters sau khi nhìn test nếu muốn tiếp tục gọi tập đó là holdout độc lập.
