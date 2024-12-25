import pandas as pd
import mysql.connector
from mysql.connector import Error
import re

try:
    # MySQL 연결 설정
    connection = mysql.connector.connect(
        host="127.0.0.1",     
        port=10003,          
        user="sesac",       
        password="5555",     
        database="sesac"         
    )
    
    if connection.is_connected():
        cursor = connection.cursor()
        print("connect successful!!")

        #dataframe 저장
        query= "SELECT * FROM sesac.spot_business_hour_info;"
        df = pd.read_sql_query(query, connection)
        
except Error as e:
    print(f"오류 발생: {e}")

finally:
    # 연결 종료
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL 연결이 종료되었습니다.")

#data 전처리
#spot_business_hour
#정규표현식
open_close_pattern = r"((월|화|수|목|금|토|일)?요일|평일|주말|매일) (\d{2}:\d{2})~(\d{2}:\d{2})" 
breaktime_pattern = r"(브레이크타임) (\d{2}:\d{2})~(\d{2}:\d{2})"
holiday_pattern = r"(매주\s*)?(\S*요일)\s*휴무"
last_order_pattern = r"(?i)(last order|라스트 주문)\s*(\d{2}:\d{2})"

summaries = df['summary'].tolist()

#전처리 후 data 넣을 새 DataFrame 생성
new_df = pd.DataFrame(columns=['spot_business_hour_id', 'spot_business_hour_info_id','week', 'open_time','close_time','last_order','break_start_time','break_end_time'])

a_week_mapping = {
    "평일": ["월요일", "화요일", "수요일", "목요일", "금요일"],
    "주말": ["토요일", "일요일"],
    "매일": ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]
}

for row in df.itertuples():
    #Summary 없는 경우 -> None값
    if not row.summary or row.summary.strip() == "": 
        for day in a_week_mapping["매일"]:
            new_row = pd.DataFrame({
                'spot_business_hour_info_id': row.spot_business_hour_info_id,
                'week': [day],
                'open_time': [None],
                'close_time': [None],
                'last_order': [None],
                'break_start_time': [None],
                'break_end_time': [None],
            })
            new_df = pd.concat([new_df, new_row], ignore_index=True)
        continue  

    
    #Summary 있는 경우 
    blocks = re.split(r"\|", row.summary)
    
    for block in blocks:
        block = block.strip()
        
        open_close_matches = re.findall(open_close_pattern, block)
        breaktime_matches = re.findall(breaktime_pattern, block)
        holiday_matches = re.findall(holiday_pattern, block)
        last_order_matches = re.findall(last_order_pattern, block)
        print('last_order_matches:',last_order_matches)
        
        break_start, break_end = None, None
        if breaktime_matches:
            break_start, break_end = breaktime_matches[0][1], breaktime_matches[0][2] 
        
        last_order = last_order_matches[0][1] if last_order_matches else None

        # 휴무 요일 처리
        holiday_days = set() 
        for holiday in holiday_matches:
            day = holiday[1]
            if day in a_week_mapping:
                a_week = a_week_mapping[day]
            else:
                a_week = [day]
            
            for day in a_week:
                holiday_days.add(day) 
                new_row = pd.DataFrame({
                    'spot_business_hour_info_id': row.spot_business_hour_info_id,
                    'week': [day],
                    'open_time': [None],
                    'close_time': [None],
                    'last_order': [None],
                    'break_start_time': [None],
                    'break_end_time': [None]
                })
                new_df = pd.concat([new_df, new_row], ignore_index=True)

        # 영업 시간 처리
        for open_close_match in open_close_matches:
            day_or_category = open_close_match[0]
            start_time = open_close_match[2]
            end_time = open_close_match[3]
            
            if day_or_category in a_week_mapping:
                a_week = a_week_mapping[day_or_category]
            else:
                a_week = [day_or_category]
            
            for day in a_week:
                if day in holiday_days:  # 휴무 요일은 건너뜀
                    continue
                
                new_row = pd.DataFrame({
                    'spot_business_hour_info_id': row.spot_business_hour_info_id,
                    'week': [day],
                    'open_time': [start_time],
                    'close_time': [end_time],
                    'last_order': [last_order],
                    'break_start_time': [break_start],
                    'break_end_time': [break_end]
                })
                new_df = pd.concat([new_df, new_row], ignore_index=True)


#Time형식 변환
new_df['open_time'] = pd.to_datetime(new_df['open_time'], format='%H:%M', errors='coerce').dt.time
new_df['close_time'] = pd.to_datetime(new_df['close_time'], format='%H:%M', errors='coerce').dt.time
new_df['break_start_time'] = pd.to_datetime(new_df['break_start_time'], format='%H:%M', errors='coerce').dt.time
new_df['break_end_time'] = pd.to_datetime(new_df['break_end_time'], format='%H:%M', errors='coerce').dt.time
new_df['last_order'] = pd.to_datetime(new_df['last_order'], format='%H:%M', errors='coerce').dt.time
new_df['spot_business_hour_info_id'] = pd.to_numeric(new_df['spot_business_hour_info_id'], errors='coerce')

# 중복 요일 rows(row 7개 이상) breaktime 나누기
count_by_spot_id = new_df.groupby('spot_business_hour_info_id').size()
spot_ids_with_more_than_7 = count_by_spot_id[count_by_spot_id > 7].index
#spot_ids_with_more_than_7:  Index([2, 9, 13, 14, 25, 39, 45], dtype='int64', name='spot_business_hour_info_id')

filtered_df = new_df[new_df['spot_business_hour_info_id'].isin(spot_ids_with_more_than_7)]
result = []

for spot_id in spot_ids_with_more_than_7:
    spot_data = filtered_df[filtered_df['spot_business_hour_info_id'] == spot_id]  
    for week in spot_data['week'].unique():
        week_data = spot_data[spot_data['week'] == week]  

        min_open_time = week_data['open_time'].dropna().min() 
        max_close_time = week_data['close_time'].dropna().max()
        
        times = pd.concat([week_data['open_time'], week_data['close_time']]).dropna().sort_values().unique()
        if len(times) >= 4:  
            mid_start_time = times[1]  
            mid_end_time = times[2]  
        else:  
            mid_start_time = None
            mid_end_time = None

        result.append({
            'spot_business_hour_info_id':spot_id,
            'week': week,
            'open_time': min_open_time,
            'close_time': max_close_time,
            'break_start_time': mid_start_time,
            'break_end_time': mid_end_time
        })

result_df = pd.DataFrame(result)
new_df = new_df[~new_df['spot_business_hour_info_id'].isin(spot_ids_with_more_than_7)]

new_df = pd.concat([new_df, result_df], ignore_index=True)


#data정렬(spot_business_hour_id기준, week기준)
week_order = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]
new_df['week'] = pd.Categorical(new_df['week'], categories=week_order, ordered=True)
new_df = new_df.sort_values(by=['spot_business_hour_info_id', 'week']).reset_index(drop=True)

new_df['spot_business_hour_id'] = new_df.index + 1

#spot_business_info
#휴일데이터, 연중무휴 데이터 정규표현식
irregular_closed_pattern = r"((?:\d+(?:,\d+)*\s*째주|\b첫째|\b둘째|\b셋째|\b넷째|\b마지막|매주|명절(?: 당일)?| 공휴일)?\s*(?:월|화|수|목|금|토|일)?(?:요일)?(?:은|이)?)\s*(휴무|정기휴무|영업 안 함)"
round_the_clock_pattern = r"연중무휴"

for index, row in df.iterrows():
    summary_text = row.get('summary', '')

    irregular_closed_matches = re.findall(irregular_closed_pattern, summary_text)
    print(irregular_closed_matches)
    if irregular_closed_matches:
        irregular_closed_info = f"{', '.join([' '.join(match) for match in irregular_closed_matches])}"
    else:
        irregular_closed_info = None

    round_the_clock_match = bool(re.search(round_the_clock_pattern, summary_text))
    round_the_clock_info = "연중무휴" if round_the_clock_match else None

    extracted_info = [info for info in [irregular_closed_info, round_the_clock_info] if info]
    df.loc[index, 'info'] = " | ".join(extracted_info) if extracted_info else None

print(df.head(50))

#전처리 데이터 DB에 저장
try:
    connection = mysql.connector.connect(
        host="127.0.0.1",
        port=10003,
        user="sesac",
        password="5555",
        database="sesac"
    )
    
    if connection.is_connected():
        cursor = connection.cursor()
        print("connect successful!!")
        
        # spot_business_hour 테이블에 new_df 저장
        for _, row in new_df.iterrows():
            sql = """
            INSERT INTO spot_business_hour 
            (spot_business_hour_id, spot_business_hour_info_id, week, open_time, close_time, break_start_time, break_end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, tuple(row))
        
        # spot_business_hour_info 테이블에 df 저장
        for _, row in df.iterrows():
            sql = """
            INSERT INTO spot_business_hour_info
            (spot_business_hour_info_id, summary, other_column1, other_column2) -- 컬럼명을 실제 테이블에 맞게 수정하세요.
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sql, tuple(row))
        
        connection.commit()
        print("데이터 저장 완료!")
        
except Error as e:
    print(f"오류 발생: {e}")

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL 연결이 종료되었습니다.")
