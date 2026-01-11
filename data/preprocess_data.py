"""
전처리 스크립트: CSV 파일을 타임스탬프 순으로 정렬하여 저장

사용법:
    cd c:/Users/USER/Desktop/bootcamp/project/Clickstream-Guardian/data
    python preprocess_data.py
"""
import csv
import sys
from datetime import datetime


def sort_csv_by_timestamp(input_file, output_file, timestamp_column='Timestamp'):
    """CSV 파일을 타임스탬프 순으로 정렬"""
    print(f"📖 Loading {input_file}...")
    
    rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for i, row in enumerate(reader):
            rows.append(row)
            if (i + 1) % 1000000 == 0:
                print(f"   Read {i + 1:,} rows...")
    
    print(f"✅ Loaded {len(rows):,} rows")
    print(f"🔄 Sorting by {timestamp_column}...")
    
    # Sort by timestamp
    rows.sort(key=lambda r: r[timestamp_column])
    
    print(f"💾 Writing to {output_file}...")
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, row in enumerate(rows):
            writer.writerow(row)
            if (i + 1) % 1000000 == 0:
                print(f"   Wrote {i + 1:,} rows...")
    
    print(f"✅ Sorted file saved: {output_file}")
    
    # Show time range
    first_ts = datetime.fromisoformat(rows[0][timestamp_column].replace('Z', '+00:00'))
    last_ts = datetime.fromisoformat(rows[-1][timestamp_column].replace('Z', '+00:00'))
    print(f"📅 Time range: {first_ts} to {last_ts}")


if __name__ == '__main__':
    print("="*80)
    print("🚀 CSV Preprocessing - Sort by Timestamp")
    print("="*80)
    
    # Clicks
    print("\n1️⃣ Processing clicks...")
    sort_csv_by_timestamp(
        'yoochoose-clicks.dat',
        'yoochoose-clicks-sorted.dat',
        'Timestamp'
    )
    
    # Purchases
    print("\n2️⃣ Processing purchases...")
    sort_csv_by_timestamp(
        'yoochoose-buys.dat',
        'yoochoose-buys-sorted.dat',
        'Timestamp'
    )
    
    print("\n" + "="*80)
    print("✅ Preprocessing complete!")
    print("="*80)
