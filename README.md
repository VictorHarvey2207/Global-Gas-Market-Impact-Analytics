# Global-Gas-Market-Impact-Analytics

Pipeline batch dùng **PySpark và Spark MLlib để dự báo giá dầu ở phiên quan sát kế tiếp**. Đây là phạm vi hiện tại của môn Big Data Storage and Processing; tên repository được giữ nguyên. Chưa triển khai phân tích tác động nhân quả hoặc dự báo giá khí tự nhiên.

## Phạm vi

- Đọc CSV theo schema xác định; kiểm tra ngày, số liệu, dữ liệu trùng và dữ liệu thiếu.
- Tạo lag, trung bình trượt, độ lệch chuẩn và biến động giá bằng Spark SQL windows.
- Chia train/validation/test theo thời gian; fit preprocessing chỉ trên tập được phép.
- Tuning **LinearRegression** và **DecisionTreeRegressor** bằng validation theo thời gian.
- So sánh với **persistence**: giá phiên sau bằng giá phiên hiện tại.
- Lưu Parquet, Spark PipelineModel, tham số, metrics, giải thích mô hình và thông tin nguồn dữ liệu.
- Batch inference dùng lại đúng preprocessing đã lưu.

Kafka, HDFS, database và Docker/deployment là **future work**, chưa có connector, dịch vụ hoặc cấu hình triển khai trong repo.

## Dữ liệu và ý nghĩa bài toán

File mặc định là `data/raw/oil_data.csv`, sao chép nguyên trạng từ [Project_DeepLearning_2025_1 tại commit 8474917](https://github.com/KietLe2504/Project_DeepLearning_2025_1/tree/8474917c5c62c1bb9c8acc3f5543b56e94f72604).

- 5.757 dòng, 10 cột, từ **2003-01-02 đến 2025-11-26**.
- Nhãn gốc mang tên `cushing_crude_oil_price`; code thu thập của repo nguồn sử dụng Yahoo Finance `CL=F`, nên hiểu đây là chuỗi liên quan đến **WTI futures**, không tự khẳng định là giá spot Cushing.
- Giữ giá ở thang gốc USD/thùng, bao gồm giá âm ngày 2020-04-20. Không log, không tự loại outlier, không đổi sang phần trăm tăng trưởng.
- Tám biến kinh tế: Dow Jones, Nasdaq, S&P, tỷ giá USD/GBP theo tên cột nguồn, federal funds, bank prime, Treasury 1 năm và breakeven inflation 5 năm. Tên cột được giữ nguyên; chưa xác minh lại định nghĩa/chiều tỷ giá từ nhà cung cấp.
- File này **không chứa Henry Hub**. Mở rộng thêm biến khí là thay đổi dữ liệu/feature contract trong một đợt sau.
- Không dùng `compiled_dataset.csv` mặc định: file đó đã biến đổi log, thiếu quy trình tái tạo đầy đủ và khác biệt với dữ liệu thô tại ngày giá âm. Loader từ chối schema của file compiled để tránh trộn thang đo.

Tại thời điểm đóng cửa phiên `t`, dùng thông tin lịch sử đến `t` để dự đoán giá ở dòng quan sát tiếp theo `t+1`. Một phiên không nhất thiết là một ngày lịch. Dự đoán cho phiên kế tiếp của dòng cuối không tự gán ngày cụ thể vì repo chưa có lịch giao dịch.

Chi tiết nguồn, chất lượng và giới hạn: [docs/data.md](docs/data.md).

## Môi trường

Python **3.10–3.13**, Java **17+**, PySpark **4.0.1**. Khuyến nghị Linux hoặc WSL2 có môi trường Linux riêng. Chưa xác nhận chạy trực tiếp trên Windows; các thao tác filesystem của Spark/Hadoop có thể cần thành phần native bổ sung.

```bash
java -version
python --version
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Đặt `JAVA_HOME` trỏ tới JDK nếu `java` chưa được tìm thấy. Spark local mặc định dùng 2 luồng; không cần dựng Spark cluster. Cấu hình `spark_master: null` để nhường lựa chọn master cho môi trường submit khi mở rộng sau này.

Tham chiếu chính thức: [cài PySpark 4.0.1](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html), [LinearRegression](https://spark.apache.org/docs/4.0.1/api/python/reference/api/pyspark.ml.regression.LinearRegression.html), [DecisionTreeRegressor](https://spark.apache.org/docs/4.0.1/api/python/reference/api/pyspark.ml.regression.DecisionTreeRegressor.html).

## Chạy pipeline

Chạy từ thư mục gốc repo, sau khi cài package:

```bash
gas-analytics run --config config/default.json --run-id experiment-01
```

Lệnh này thực hiện preprocessing rồi training. Không ghi đè kết quả có sẵn; dùng một `run-id` mới khi chạy lại. Chỉ thư mục có `manifest.json` với `status: complete` mới là artifact hoàn tất.

Có thể chạy riêng từng bước:

```bash
gas-analytics preprocess --config config/default.json --run-id prepared-01
gas-analytics train --config config/default.json --prepared artifacts/prepared/prepared-01 --run-id model-01
gas-analytics predict --run artifacts/runs/model-01 --input data/raw/oil_data.csv
```

`predict` mặc định dùng model được chọn bằng **validation RMSE**, có thể là persistence nếu ML không tốt hơn. Có thể chỉ định `--model linear_regression` hoặc `--model decision_tree` để kiểm tra riêng. File đầu vào inference phải chứa lịch sử đủ dài, tối thiểu 20 quan sát với config mặc định; nên cung cấp lịch sử đầy đủ, nhất là khi có dữ liệu thiếu.

“Latest” là dòng cuối của file đầu vào, không phải giá thị trường hôm nay. Với snapshot đi kèm, thời điểm gốc của dự đoán vẫn là 2025-11-26.

Đường dẫn trong config được tính **tương đối với file config**, không phụ thuộc thư mục gọi lệnh. Đường dẫn `--prepared`, `--run`, `--input` trên CLI được tính theo thư mục gọi lệnh. Khi di chuyển artifacts sang máy khác, dùng `predict --input ... --output-dir ...` để thay đường dẫn nguồn/đích đã lưu.

```text
artifacts/
  prepared/<id>/
    train/                 # Parquet: đặc trưng, nhãn, origin_date, target_date
    validation/
    test/
    manifest.json          # Profile, SHA-256 nguồn, schema/feature contract, khoảng thời gian
  runs/<id>/
    linear_regression/     # PipelineModel: median imputer + assembler + estimator
    decision_tree/
    test_predictions/      # Parquet: kết quả của cả 3 phương pháp
    metrics.json
    tuning.json
    explanations.json
    manifest.json
  predictions/<id>/prediction.json
```

Artifacts là thư mục local. Việc reader nhận URI mà Spark hiểu không đồng nghĩa connector HDFS đã được triển khai/kiểm thử. Khi lỗi giữa chừng, thư mục chưa hoàn tất có thể còn lại; chọn ID mới sau khi sửa lỗi.

## Quy tắc preprocessing và đánh giá

1. Kiểm tra schema, ngày hợp lệ, date duy nhất, số hữu hạn; không tự sửa dữ liệu lỗi. Nhãn thiếu bị từ chối; covariate thiếu được cho phép.
2. Không resample cuối tuần, không interpolate/backfill từ tương lai.
3. Dùng giá hiện tại; lag 1, 2, 5, 10 phiên; rolling mean/std 5, 10, 20 phiên; chênh lệch giá 1 phiên. Rolling windows bao gồm phiên hiện tại vì dự báo được lập sau đóng cửa.
4. Covariate mặc định trễ 1 phiên; forward-fill tối đa 5 phiên tính từ thời điểm được phép quan sát. Sau đó giữ null và thêm missing indicator. Median imputer học trên train giải quyết phần còn thiếu.
5. Bỏ 19 dòng warmup và 1 dòng cuối chưa có nhãn; chia khoảng 70%/15%/15% theo **target_date**. Không `randomSplit`, không K-fold ngẫu nhiên.
6. Fit imputer và model trên train, chọn tham số theo validation. Spark LinearRegression dùng internal standardization; Decision Tree không cần MinMaxScaler.
7. Chọn phương pháp bằng validation, rồi refit các model ML tốt nhất trên train + validation; đánh giá test cuối cùng. Test không được dùng để chọn champion.
8. Báo cáo RMSE và MAE ở thang USD/thùng, R² và mức cải thiện RMSE so với persistence. Không dùng MAPE vì giá có thể âm hoặc gần 0. R² không phải tỷ lệ dự báo chính xác.

Lưới tham số mặc định trong `config/default.json`:

| Model | Tham số | Lý do khởi đầu |
|---|---|---|
| Linear Regression | `regParam`: 0, 0.1, 1; `elasticNetParam`: 0, 0.5; `maxIter`: 200 | So sánh OLS, ridge và elastic net khi các lag/rolling features tương quan cao; bỏ lặp trường hợp reg=0 |
| Decision Tree | `maxDepth`: 3, 5, 8; `minInstancesPerNode`: 10, 30; `maxBins`: 32 | Hạn chế overfit với tập dữ liệu vài nghìn dòng; seed 42 |

Đây là lưới tìm kiếm, không phải khẳng định tham số tối ưu phổ quát. Một holdout thời gian là baseline thực nghiệm; walk-forward validation và dữ liệu theo thời điểm công bố là phần cần bổ sung trước khi đánh giá khả năng vận hành thực tế. Validation/test là mô phỏng dự báo từng bước: các quan sát thật đã xuất hiện trước thời điểm dự báo được phép dùng làm lịch sử; không phải dự báo nhiều bước từ một mốc cố định.

## Kiểm thử

```bash
python -m ruff check src tests
python -m pytest -q
```

Tests chạy Spark thật và kiểm tra tính nhân quả của features, horizon qua cuối tuần, giới hạn forward-fill, train-only imputation, split theo thời gian, giá âm, dữ liệu lỗi và save/reload/inference parity. Không cần network để chạy tests sau khi đã cài dependencies.

Kết quả chạy trên toàn bộ snapshot: [docs/results.md](docs/results.md). Lần chạy đầu chọn Linear Regression theo validation; **persistence vẫn tốt hơn cả hai model ML trên test**. Không diễn giải việc dùng mô hình phức tạp hơn là mặc nhiên chính xác hơn.

## Cấu trúc và mở rộng

```text
src/gas_analytics/
  config.py        # Cấu hình và hợp đồng dữ liệu
  runtime.py       # Tạo SparkSession
  data.py          # Đọc, kiểm tra và profile dữ liệu thô
  features.py      # Tạo đặc trưng và chia theo thời gian
  artifacts.py     # Preprocessing artifacts và metadata local
  training.py      # Tuning, refit, evaluation và model persistence
  prediction.py    # Nạp model và dự đoán từ lịch sử mới
  cli.py           # Điều phối các bước
```

Các hàm transformation nhận/trả Spark DataFrame, không khởi tạo Kafka/DB bên trong model. API làm việc với dữ liệu khác và các storage adapter có thể bổ sung ở biên nhập/xuất. [docs/architecture.md](docs/architecture.md) mô tả hợp đồng bàn giao cho thành viên phụ trách tích hợp. Không hứa rằng streaming sẽ chỉ cần thay URI: event-time, dữ liệu đến muộn và trạng thái rolling windows cần thiết kế riêng.

**Giới hạn Big Data:** 5.757 dòng chưa phải dữ liệu lớn. Các window của một chuỗi duy nhất dùng global ordering và có thể gom về một partition. Pipeline dùng Spark API để học quy trình; chưa chứng minh khả năng scale-out. Nhiều mã hàng hóa/thị trường, partition theo instrument và benchmark dữ liệu lớn là công việc tiếp theo.
