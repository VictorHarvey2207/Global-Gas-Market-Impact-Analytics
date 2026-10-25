# Kết quả baseline ngày 2026-10-25

Run ID: `baseline-20261025`. Đây là kết quả thực thi trên snapshot đi kèm, không phải số liệu từ notebook Deep Learning. Bản ghi máy đọc được: [reports/baseline-20261025.json](../reports/baseline-20261025.json).

## Môi trường đã kiểm thử

- Spark 4.0.1, Java 17.0.16, Python 3.10.12, NumPy 1.26.4.
- Runtime Linux tạm từ image chính thức `apache/spark:4.0.1-python3`, digest `sha256:acfe7e06e95dd13aa32ee03c464766900d52fba15f58e421f8e1213ec041bb94`.
- Docker chỉ được dùng làm môi trường kiểm thử trên máy Windows; không bổ sung hạ tầng deployment vào repo.
- 10 tests pass; Ruff pass; full preprocessing/training và CLI inference từ model đã lưu chạy thành công.
- Chưa kiểm chứng native Windows hoặc cluster phân tán; chưa chạy benchmark hiệu năng Big Data.

## Phân chia dữ liệu

Từ 5.757 dòng: bỏ 19 dòng warmup và 1 dòng cuối chưa có nhãn, còn 5.737 mẫu. Ranh giới dựa trên **ngày của nhãn**.

| Tập | Số mẫu | Ngày nhãn đầu | Ngày nhãn cuối |
|---|---:|---|---|
| Train | 4.015 | 2003-01-31 | 2019-01-24 |
| Validation | 860 | 2019-01-25 | 2022-06-23 |
| Test | 862 | 2022-06-24 | 2025-11-26 |

## Tuning và test

Đã thử 5 cấu hình Linear Regression và 6 cấu hình Decision Tree. Imputer của tuning chỉ học từ train. Sau khi chọn bằng validation, refit mỗi model ML thắng trong họ của nó trên train+validation; không thay tham số sau khi xem test.

| Phương pháp | Validation RMSE | Test RMSE | Test MAE | Test R² |
|---|---:|---:|---:|---:|
| Persistence | 3.122903 | **1.662512** | **1.268295** | **0.968613** |
| Linear Regression | **3.065550** | 1.726629 | 1.306017 | 0.966145 |
| Decision Tree | 5.547792 | 1.967181 | 1.557743 | 0.956055 |

RMSE/MAE tính bằng USD/thùng. R² không phải accuracy.

Tham số được chọn:

- Linear Regression: `regParam=0.1`, `elasticNetParam=0.5`, standardization bật, `maxIter=200`.
- Decision Tree: `maxDepth=5`, `minInstancesPerNode=30`, `maxBins=32`, seed 42.

Linear Regression được chọn trước khi xem test vì có validation RMSE thấp nhất. Tuy nhiên test RMSE của nó cao hơn persistence khoảng **3,86%**; Decision Tree cao hơn khoảng **18,33%**. Chưa có bằng chứng hai model ML này cải thiện khả năng dự báo ngoài mẫu so với baseline đơn giản trên giai đoạn test này.

Không thay champion bằng kết quả test trong cùng thí nghiệm. Nếu muốn nghiên cứu tiếp, dùng walk-forward trên giai đoạn phát triển và một holdout mới; không liên tục tuning trên test hiện tại. Tree cũng có hạn chế ngoại suy ngoài miền giá trị đã học. Chênh lệch validation/test có thể liên quan đến các chế độ thị trường khác nhau, nhưng run này chưa kiểm chứng nguyên nhân đó.

## Artifacts local

`artifacts/prepared/baseline-20261025` chứa Parquet và profile. `artifacts/runs/baseline-20261025` chứa hai PipelineModel hoàn chỉnh, predictions test, tuning records, coefficients/importances và manifest. `artifacts/validation/full-run.log` ghi quá trình chạy. Các thư mục này được git-ignore; report JSON nhỏ được giữ trong repo để tham khảo.

Đường dẫn trong manifest phản ánh môi trường chạy `/work`. Khi tái sử dụng trên máy khác, truyền `--input` và `--output-dir` thích hợp. Không cần dùng lại đường dẫn container để chạy một thí nghiệm mới bằng config mặc định của checkout hiện tại.
