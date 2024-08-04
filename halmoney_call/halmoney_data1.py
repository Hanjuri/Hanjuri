import requests
import xml.etree.ElementTree as ET
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# 서울시 공공데이터 API URL
url = 'http://openapi.seoul.go.kr:8088/4b41444d536a757235374c56554d57/xml/GetJobInfo/1/100/'
response = requests.get(url)
xml_data = response.text

# Parse XML data
root = ET.fromstring(xml_data)
job_list = []


# 직업 이름에 따라 사진 경로를 반환하는 함수 (num%9에 따라 다른 경로 반환)
def get_image_path(num: int) -> str:
    base_path = "assets/images/mainImages"
    remainder = num % 9  # num을 9로 나눈 나머지 계산
    return f"{base_path}/{remainder}.jpg"


# 각 row에서 필요한 데이터 추출하여 리스트에 추가
for index, row in enumerate(root.findall('.//row'), start=1):
    job = {}
    job['num'] = index  # 각 데이터에 순서에 따른 ID 추가
    job['request_num'] = row.find('JO_REQST_NO').text
    job['register_num'] = row.find('JO_REGIST_NO').text
    job['job_name'] = row.find('JOBCODE_NM').text
    job['address'] = row.find('BASS_ADRES_CN').text
    job['wage'] = row.find('HOPE_WAGE').text
    job['work_time_week'] = row.find('HOLIDAY_NM').text
    job['detail'] = row.find('DTY_CN').text
    job['career'] = row.find('CAREER_CND_NM').text
    job['isLiked'] = False
    job_list.append(job)

# pandas DataFrame 생성
df_work = pd.DataFrame(job_list)

# 각 행에 대해 이미지 경로 열 추가
df_work['image_path'] = df_work['num'].apply(get_image_path)

# 기타 열의 첫 번째 줄만 추출하고 job_name과 결합하는 함수 정의
def extract_first_line_and_combine(text, job_name):
    if text is not None:
        # '\n'과 '\r'를 모두 제거하고 첫 번째 줄 추출
        first_line = text.split('\n')[0].replace('\r', '')
        return f"{job_name} / {first_line}"
    return job_name

# apply 메서드를 사용하여 title 열 생성
df_work['title'] = df_work.apply(lambda row: extract_first_line_and_combine(row['detail'], row['job_name']), axis=1)

# Firebase 인증 정보 제공
cred = credentials.Certificate('C:/Users/juwan/Desktop/halmoney_데이터 호출/halmoney20-398d5-firebase-adminsdk.json')

# 이미 초기화된 경우 다시 초기화하지 않음
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

# Firestore 클라이언트 초기화
db = firestore.client()

# DataFrame을 JSON 형식으로 변환
json_data = df_work.to_dict(orient='records')

# Firebase에 데이터 추가
def add_data_to_firebase(data, collection_name):
    for item in data:
        db.collection(collection_name).add(item)

# 데이터를 Firebase에 추가
add_data_to_firebase(json_data, 'jobs')
