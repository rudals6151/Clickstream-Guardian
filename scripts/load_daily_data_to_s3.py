"""
하루 분량의 클릭/구매 데이터를 읽어서 날짜를 변환한 뒤 S3에 Parquet 형식으로 저장하는 스크립트

사용 예시:
    python load_daily_data_to_s3.py --source-date 2014-08-14 --target-date 2026-01-11 --anomaly-count 10
"""
import argparse
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random
import os
import tempfile
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, source_date, target_date, anomaly_count=10):
        self.source_date = source_date
        self.target_date = target_date
        self.anomaly_count = anomaly_count
        
        # AWS S3 설정
        self.s3_bucket = "km-data-lake"
        self.s3_client = boto3.client('s3')
        
        # 데이터 파일 경로
        self.project_root = Path(__file__).parent.parent
        self.clicks_file = self.project_root / 'data' / 'yoochoose-clicks-sorted.dat'
        self.buys_file = self.project_root / 'data' / 'yoochoose-buys-sorted.dat'
        
        logger.info(f"Source date: {source_date}, Target date: {target_date}")
        logger.info(f"Anomaly sessions to inject: {anomaly_count}")
    
    def parse_timestamp(self, ts_str, target_date_str):
        """원본 타임스탬프를 파싱하고 타겟 날짜로 변환"""
        # 원본: 2014-04-01T03:05:31.743Z
        source_dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        
        # 시간대는 그대로 유지하고 날짜만 변경
        new_dt = target_date.replace(
            hour=source_dt.hour,
            minute=source_dt.minute,
            second=source_dt.second,
            microsecond=source_dt.microsecond
        )
        
        # milliseconds since epoch 반환
        return int(new_dt.timestamp() * 1000)
    
    def generate_anomaly_sessions(self, clicks_df):
        """이상 세션 생성: 고빈도 클릭, 단일 아이템 반복 클릭"""
        anomaly_sessions = []
        
        if len(clicks_df) == 0:
            return pd.DataFrame()
        
        # 원본 데이터에서 세션 ID 범위 확인
        max_session_id = clicks_df['session_id'].max()
        
        # 이상 세션을 원본 데이터의 타임스탬프 사이사이에 섞어 넣기 위한 준비
        click_times = sorted(clicks_df['event_ts'].unique())
        time_slots = []
        
        # 타임 슬롯 생성 (전체 시간 범위를 anomaly_count + 1개로 분할)
        if len(click_times) > 1:
            min_time = min(click_times)
            max_time = max(click_times)
            interval = (max_time - min_time) / (self.anomaly_count + 1)
            time_slots = [min_time + interval * (i + 1) for i in range(self.anomaly_count)]
        else:
            # 클릭이 하나뿐이면 그 시간 근처에 몰아서 생성
            base_time = click_times[0]
            time_slots = [base_time + i * 1000 for i in range(self.anomaly_count)]
        
        for idx in range(self.anomaly_count):
            # 고유한 이상 세션 ID 생성
            anomaly_session_id = max_session_id + 1000000 + idx
            
            # 타임 슬롯 할당
            base_timestamp = int(time_slots[idx]) if idx < len(time_slots) else click_times[0]
            
            # 이상 패턴 1: 짧은 시간에 고빈도 클릭 (30-50개)
            num_clicks = random.randint(30, 50)
            item_id = random.choice([214717005, 214826705, 214716982, 214581827])
            
            # 10초 내에 클릭 분산
            for i in range(num_clicks):
                click_ts = base_timestamp + random.randint(0, 10000)  # 10초 내 랜덤
                anomaly_sessions.append({
                    'session_id': anomaly_session_id,
                    'event_ts': click_ts,
                    'item_id': item_id,
                    'category': '0',
                    'event_type': 'click'
                })
        
        anomaly_df = pd.DataFrame(anomaly_sessions)
        logger.info(f"Generated {len(anomaly_df)} anomaly clicks across {self.anomaly_count} sessions")
        
        return anomaly_df
    
    def load_clicks(self):
        """클릭 데이터 로드 및 변환"""
        logger.info(f"Loading clicks from {self.clicks_file}")
        
        # 청크 단위로 읽으면서 필터링 (메모리 효율적)
        chunks = []
        source_date_prefix = self.source_date
        
        for chunk in pd.read_csv(self.clicks_file, chunksize=100000):
            # 날짜 문자열로 빠른 필터링
            chunk_filtered = chunk[chunk['Timestamp'].str.startswith(source_date_prefix)]
            if len(chunk_filtered) > 0:
                chunks.append(chunk_filtered)
        
        if not chunks:
            logger.warning(f"No clicks found for {self.source_date}")
            return pd.DataFrame()
        
        df_filtered = pd.concat(chunks, ignore_index=True)
        logger.info(f"Filtered {len(df_filtered)} clicks for date {self.source_date}")
        
        if len(df_filtered) == 0:
            logger.warning(f"No clicks found for {self.source_date}")
            return pd.DataFrame()
        
        # 타임스탬프 변환
        df_filtered['event_ts'] = df_filtered['Timestamp'].apply(
            lambda x: self.parse_timestamp(x, self.target_date)
        )
        
        # 스키마에 맞게 컬럼 매핑
        clicks_df = pd.DataFrame({
            'session_id': df_filtered['Session ID'].astype('int64'),
            'event_ts': df_filtered['event_ts'].astype('int64'),
            'item_id': df_filtered['Item ID'].astype('int64'),
            'category': df_filtered['Category'].astype(str),
            'event_type': 'click'
        })
        
        # 이상 세션 추가
        if self.anomaly_count > 0:
            anomaly_df = self.generate_anomaly_sessions(clicks_df)
            if not anomaly_df.empty:
                clicks_df = pd.concat([clicks_df, anomaly_df], ignore_index=True)
                # 타임스탬프 순으로 재정렬
                clicks_df = clicks_df.sort_values('event_ts').reset_index(drop=True)
                logger.info(f"Total clicks after adding anomalies: {len(clicks_df)}")
        
        return clicks_df
    
    def load_purchases(self):
        """구매 데이터 로드 및 변환"""
        logger.info(f"Loading purchases from {self.buys_file}")
        
        # 청크 단위로 읽으면서 필터링 (메모리 효율적)
        chunks = []
        source_date_prefix = self.source_date
        
        for chunk in pd.read_csv(self.buys_file, chunksize=100000):
            # 날짜 문자열로 빠른 필터링
            chunk_filtered = chunk[chunk['Timestamp'].str.startswith(source_date_prefix)]
            if len(chunk_filtered) > 0:
                chunks.append(chunk_filtered)
        
        if not chunks:
            logger.warning(f"No purchases found for {self.source_date}")
            return pd.DataFrame()
        
        df_filtered = pd.concat(chunks, ignore_index=True)
        logger.info(f"Filtered {len(df_filtered)} purchases for date {self.source_date}")
        
        if len(df_filtered) == 0:
            logger.warning(f"No purchases found for {self.source_date}")
            return pd.DataFrame()
        
        # 타임스탬프 변환
        df_filtered['event_ts'] = df_filtered['Timestamp'].apply(
            lambda x: self.parse_timestamp(x, self.target_date)
        )
        
        # 스키마에 맞게 컬럼 매핑
        purchases_df = pd.DataFrame({
            'session_id': df_filtered['Session ID'].astype('int64'),
            'event_ts': df_filtered['event_ts'].astype('int64'),
            'item_id': df_filtered['Item ID'].astype('int64'),
            'price': df_filtered['Price'].astype('float64'),
            'quantity': df_filtered['Quantity'].astype('int32'),
            'event_type': 'purchase'
        })
        
        return purchases_df
    
    def save_to_s3_parquet(self, df, data_type='clicks'):
        """DataFrame을 S3에 시간별로 파티셔닝하여 Parquet 형식으로 저장"""
        if len(df) == 0:
            logger.warning(f"No data to save for {data_type}")
            return
        
        # 시간별로 그룹화
        df['datetime'] = pd.to_datetime(df['event_ts'], unit='ms')
        df['hour'] = df['datetime'].dt.hour
        
        # 데이터 타입에 따른 경로 설정
        if data_type == 'clicks':
            path_prefix = 'topics/km.clicks.raw.v1/raw_clicks'
        else:
            path_prefix = 'topics/km.purchases.raw.v1/raw_purchases'
                
        # 임시 디렉토리 생성 (Windows 호환)
        temp_dir = tempfile.mkdtemp()
        
        try:
            for hour, hour_df in df.groupby('hour'):
                # 저장할 컬럼만 선택
                if data_type == 'clicks':
                    save_df = hour_df[['session_id', 'event_ts', 'item_id', 'category', 'event_type']]
                else:
                    save_df = hour_df[['session_id', 'event_ts', 'item_id', 'price', 'quantity', 'event_type']]
                
                # S3 경로 생성 (connector와 동일한 형식)
                s3_path = f"{path_prefix}/dt={self.target_date}/hour={hour:02d}"
                
                # PyArrow Table 생성
                table = pa.Table.from_pandas(save_df, preserve_index=False)
                
                # 로컬 임시 파일에 저장
                temp_file = os.path.join(temp_dir, f'{data_type}_{hour:02d}.parquet')
                pq.write_table(table, temp_file, compression='snappy')
                
                # S3에 업로드
                s3_key = f"{s3_path}/{data_type}_{self.target_date}_{hour:02d}.parquet"
                
                try:
                    self.s3_client.upload_file(temp_file, self.s3_bucket, s3_key)
                    logger.info(f"Uploaded to s3://{self.s3_bucket}/{s3_key} ({len(save_df)} records)")
                except Exception as e:
                    logger.error(f"Failed to upload {s3_key}: {e}")
                finally:
                    # 임시 파일 삭제
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
        finally:
            # 임시 디렉토리 삭제
            try:
                os.rmdir(temp_dir)
            except:
                pass
    
    def verify_s3_data(self):
        """S3에 저장된 데이터 확인"""
        logger.info("\n" + "="*60)
        logger.info("Verifying S3 data...")
        logger.info("="*60)
        
        for data_type in ['clicks', 'purchases']:
            if data_type == 'clicks':
                path_prefix = 'topics/km.clicks.raw.v1/raw_clicks'
            else:
                path_prefix = 'topics/km.purchases.raw.v1/raw_purchases'
            prefix = f"{path_prefix}/dt={self.target_date}/"
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=prefix
                )
                
                if 'Contents' in response:
                    files = response['Contents']
                    total_size = sum(f['Size'] for f in files)
                    
                    logger.info(f"\n{data_type.upper()}:")
                    logger.info(f"  Path: s3://{self.s3_bucket}/{prefix}")
                    logger.info(f"  Files: {len(files)}")
                    logger.info(f"  Total size: {total_size / 1024:.2f} KB")
                    
                    # 각 파일 상세 정보
                    for f in files:
                        logger.info(f"    - {f['Key'].split('/')[-1]}: {f['Size']} bytes")
                else:
                    logger.warning(f"No files found for {data_type} at {prefix}")
            except Exception as e:
                logger.error(f"Error verifying {data_type}: {e}")
    
    def run(self):
        """메인 실행 함수"""
        logger.info("Starting data load process...")
        
        # 클릭 데이터 처리
        clicks_df = self.load_clicks()
        if not clicks_df.empty:
            self.save_to_s3_parquet(clicks_df, 'clicks')
        
        # 구매 데이터 처리
        purchases_df = self.load_purchases()
        if not purchases_df.empty:
            self.save_to_s3_parquet(purchases_df, 'purchases')
        
        # 검증
        self.verify_s3_data()
        
        logger.info("\nData load completed successfully!")


def main():
    parser = argparse.ArgumentParser(
        description='Load daily click/purchase data and save to S3 with date transformation'
    )
    parser.add_argument(
        '--source-date',
        type=str,
        required=True,
        help='Source date to load data from (YYYY-MM-DD), e.g., 2014-04-01'
    )
    parser.add_argument(
        '--target-date',
        type=str,
        required=True,
        help='Target date to save data as (YYYY-MM-DD), e.g., 2026-01-11'
    )
    parser.add_argument(
        '--anomaly-count',
        type=int,
        default=10,
        help='Number of anomaly sessions to inject (default: 10)'
    )
    
    args = parser.parse_args()
    
    # 날짜 형식 검증
    try:
        datetime.strptime(args.source_date, "%Y-%m-%d")
        datetime.strptime(args.target_date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD")
        return
    
    # 데이터 로더 실행
    loader = DataLoader(args.source_date, args.target_date, args.anomaly_count)
    loader.run()


if __name__ == "__main__":
    main()
