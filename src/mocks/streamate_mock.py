""" Codigo generador de Mocks para Streamate """
import json
import random
from datetime import datetime, timedelta

# PROMEDIO MENSUAL: 10.000 (6.000 JASMIN - 4.000 DE STREAMATE) DESV 30%.
# FECHA: En el dia de ejecucion.


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


def generate_performer(performer_id, performers):
    nickname = random.choice(performers)
    email_address = f"{nickname.lower().replace(' ', '')}@models1a.com"
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


def generate_studio(studio_id, performers):
    email_address = f"studio{studio_id}@models1a.com"
    earnings = generate_earnings(5)
    performers = [generate_performer(
        studio_id * 1000 + i, performers) for i in range(3)]
    return {
        "studioId": studio_id,
        "emailAddress": email_address,
        "earnings": earnings,
        "performers": performers
    }


def generate_data(num_studios, performers):
    studios = [generate_studio(i, performers) for i in range(num_studios)]
    data_timestamp = int(datetime.now().timestamp())
    return {
        "studios": studios,
        "data_timestamp": data_timestamp
    }


if __name__ == "__main__":
    num_studios = 5
    performers = ["Carla Ferrara", "Sara Vogel", "Alejandra Roa", "Rebeca Villalobos", "Ninna Portugal", "Jessica Portman",
                  "Rafaela Luna", "Sofia Kaufman", "Karina Goldman", "Jessy Cusack", "Vicky Portman", "Mara Kovalenko"]
    data = generate_data(num_studios, performers)
    with open(f"{datetime.now()}.json", "w") as json_file:
        json.dump(data, json_file, indent=2)

    print(":: Data exported Succesfully ::")
