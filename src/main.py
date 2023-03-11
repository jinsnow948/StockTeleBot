import re
import asyncio
import json

import pytz
from telethon import TelegramClient
from datetime import datetime, timedelta

import db_handle

# 사용자 API ID 및 해시
with open('../config/config.json') as f:
    config = json.load(f)

api_id = config['api_id']
api_hash = config['api_hash']
session_name = config['session_name']

# 텔레그램 클라이언트 인스턴스 생성
client = TelegramClient(session_name, api_id, api_hash)


async def get_messages(channel_name):
    # 채널 정보 가져오기
    channel_entity = await client.get_entity(channel_name)
    channel_id = channel_entity.id

    messages = []
    # messages = await client.get_messages(channel_id, limit=message_count)
    async for message in client.iter_messages(channel_id):
        kst_datetime = datetime.fromtimestamp(message.date.timestamp(), tz=kst_timezone)
        kst_time = kst_datetime.strftime("%Y-%m-%d %H:%M:%S")
        messages.append((kst_time, message.text))
        if kst_datetime.timestamp() < start_date.timestamp():
            break;
    return messages


async def bot_main(con, chn_list):
    await client.start()
    for chn in chn_list:
        messages = await get_messages(chn)
        print(f' messages : {messages}')
        message_list = []
        title = ""
        for date, message in messages:
            if chn == "wemakebull":
                title, name, contents, link = extract_wmb_msg(message)
            elif chn == "characteristicstock":
                result = extract_cts_msg(message)
                if not result:
                    continue
                title, name, link = result

            print(f'날짜 : {date} , 타이틀 : {title}, 종목명 : {name}, 이슈내용 : {contents}, 링크 : {link}, 채널명 : {chn}')
            if title and name:
                message_list.append((date, title, name, contents, link, chn))
            # print(f'날짜 : {date} , 내용 : {message}')

        if message_list:
            print(f'news_list : {message_list}')
            values = ','.join(
                map(lambda x: f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}', '{x[5]}')", message_list))
            sql = f"INSERT INTO stock_issues (report_time, title, stock_name, news_content, news_link, channel_name) " \
                  f"VALUES {values}"
            print(f'insert query {sql}')
            db_handle.execute_insert_query(con, sql)
    await client.disconnect()


def extract_wmb_msg(message):
    """
    주어진 문자열에서 종목명과 이슈내용 패턴을 추출하는 함수
    :param message: 주어진 문자열
    :return: 추출된 종목명과 이슈내용
    """

    # 이슈 타이틀 추출
    try:
        issue_title = re.findall(r"✅(.+)", message)
    except TypeError:
        issue_title = []

    # 종목명 추출
    try:
        # stock_name = re.findall(r"\[(.+)\]", message)
        # stock_name = re.findall(r"\[(?!\d).*?\]", message)
        # stock_name = [re.search(r"\[(?!\d)(.+?)\]", s).group(1) for s in re.findall(r"\[(?!\d).*?\]", message)]
        stock_name = list(
            set([re.search(r"\[(?!\d)(.+?)\]", s).group(1) for s in re.findall(r"\[(?!\d).*?\]", message)]))
    except TypeError:
        stock_name = []

    # 링크 추출
    try:
        link = re.findall(r"(https?://\S+)", message)
    except TypeError:
        link = []

    # 이슈 내용 추출
    try:
        issue_content = re.sub(r"✅(.+)", "", message)
        issue_content = re.sub(r"\[(?!\d)(.+?)\]", "", issue_content)
        issue_content = re.sub(r"(https?://\S+)", "", issue_content)
        issue_content = re.sub(r"\*\*", "", issue_content)  # ** 제거
        issue_content = re.sub(r"\n+", "/", issue_content).lstrip("/").rstrip("/")  # 뉴라인을 /로 변경
        issue_content = issue_content.strip()
    except TypeError:
        issue_content = ""

    title_str = '/'.join(issue_title).replace("'", "''")
    name_str = ','.join(stock_name).replace("'", "''")
    contents = issue_content.replace("'", "''").replace("‘", "''")
    link_str = ','.join(link)
    # 타이틀,종목명,내용,링크
    return title_str, name_str, contents, link_str


def extract_cts_msg(message):
    pattern = r"\[특징주\]\s*(.*?),\s*(.*?)\n(https?://\S+)"
    # pattern = r"\[특징주\]\s*(.*?),\s*(.*?)(\n(http\S+))?"
    match = re.search(pattern, message)

    if match:
        stock_name = match.group(1)
        title = match.group(2)
        link = match.group(3) if match.group(3) else None
        print(f'타이틀 :{ title}, 종목명 :{stock_name}, 링크 : {link}')
        # 종목명,내용,링크
        stock_name = stock_name.replace("'", "''")
        title = title.replace("'", "''")

        return title, stock_name, link
    else:
        return None


if __name__ == '__main__':
    # 검색기간
    kst_timezone = pytz.timezone('Asia/Seoul')
    end_date = datetime.now() - timedelta(days=config['end_date'])
    start_date = end_date - timedelta(days=config['start_date'])
    print(f'start_date : {start_date}, end_date : {end_date}')
    # message_count = 60

    confirmation = input(f"DROP the tables 'stock_issues'? (y/n): ")
    # MySQL 연결 설정
    conn = db_handle.connect_db()

    # 테이블 삭제 유무
    if confirmation.lower() == 'y':
        db_handle.drop_tables_stock_issues(conn)
    else:
        print(f"stock_issues will be used.")

    # 테이블 생성
    db_handle.create_table_stock_issues(conn)

    channel1 = config['channel_name1']
    channel2 = config['channel_name2']

    channel_list = [channel1, channel2]
    # channel_list = [channel2]

    asyncio.run(bot_main(conn, channel_list))
