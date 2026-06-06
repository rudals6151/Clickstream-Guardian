# 테스트 계획 및 결과 문서

이 폴더는 Clickstream Guardian의 부하 테스트, 장애 테스트, 데이터 품질 테스트 계획과 결과를 기록하기 위한 공간입니다.

진행 원칙:

1. 각 테스트는 목적, 데이터 규모, 실행 명령, 성공 기준을 먼저 정의한다.
2. 테스트 실행 후 결과 수치와 실패 원인을 기록한다.
3. 병목이나 실패가 확인되면 설정/코드를 보완한다.
4. 보완 후 같은 테스트를 재실행해 개선 여부를 기록한다.
5. 한 테스트가 정리되면 다음 테스트로 넘어간다.

테스트 문서:

- [01 API 부하 테스트](01_api_load_test.md)
- [02 Kafka Producer 처리량 테스트](02_kafka_producer_throughput.md)
- [03 Spark Streaming SLO 및 복구 테스트](03_spark_streaming_slo_recovery.md)
- [04 Kafka Connect / MinIO 적재 테스트](04_kafka_connect_minio_ingestion.md)
- [05 Airflow Batch E2E 테스트](05_airflow_batch_e2e.md)
- [06 장애 복구 매트릭스 테스트](06_failure_recovery_matrix.md)
- [07 데이터 품질 테스트](07_data_quality_checks.md)

