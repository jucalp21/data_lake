""" Codigo generador de Mocks """
import json
import random
from datetime import datetime, timedelta


def generate_random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )


def generate_earnings(num_entries):
    earnings = []
    for _ in range(num_entries):
        date = generate_random_date(datetime(2021, 1, 1), datetime(
            2021, 12, 31)).strftime('%Y-%m-%d')
        payable_amount = round(random.uniform(1, 10), 2)
        earnings.append({
            "date": date,
            "payableAmount": payable_amount
        })
    return earnings


def generate_performer(performer_id):
    nickname = f"Performer{performer_id}"
    email_address = f"{nickname.lower()}@example.com"
    earnings = generate_earnings(5)
    performer_earnings = [
        {
            "date": earning["date"],
            "onlineSeconds": random.randint(0, 10000),
            "payableAmount": earning["payableAmount"]
        } for earning in earnings
    ]
    return {
        "performerId": performer_id,
        "nickname": nickname,
        "emailAddress": email_address,
        "earnings": performer_earnings
    }


def generate_studio(studio_id):
    email_address = f"studio{studio_id}@example.com"
    earnings = generate_earnings(5)
    performers = [generate_performer(studio_id * 1000 + i) for i in range(3)]
    return {
        "studioId": studio_id,
        "emailAddress": email_address,
        "earnings": earnings,
        "performers": performers
    }


def generate_data(num_studios):
    studios = [generate_studio(i) for i in range(num_studios)]
    data_timestamp = int(datetime.now().timestamp())
    return {
        "studios": studios,
        "data_timestamp": data_timestamp
    }


if __name__ == "__main__":
    num_studios = 2
    data = generate_data(num_studios)
    print(json.dumps(data, indent=4))
