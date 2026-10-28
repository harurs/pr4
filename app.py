import asyncio
import json
from datetime import datetime

import aiohttp

MAKES_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMake/{make}?format=json"
TYPES_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/GetVehicleTypesForMake/{make}?format=json"

CAR_MAKES = ["toyota", "bmw", "audi", "honda"]
RUNS = 3
INTERVAL = 10
semaphore = asyncio.Semaphore(3)

async def fetch_data(url, session, retries=2):
    for attempt in range(1, retries + 2):
        async with semaphore:
            try:
                response = await asyncio.wait_for(session.get(url), timeout=5)
                async with response:
                    if response.status == 200:
                        data = await response.json(content_type=None)
                        return data.get("Results", [])
                    print(f"Ошибка {response.status} при запросе {url}")
            except asyncio.TimeoutError:
                print(f"Таймаут при запросе {url} (попытка {attempt})")
            except aiohttp.ClientError as e:
                print(f"Ошибка подключения {e} при запросе {url} (попытка {attempt})")
            except json.JSONDecodeError:
                print(f"Некорректный JSON от {url}")
                return None
        await asyncio.sleep(5)
    return None

async def get_car_info(make, session):
    models, types = await asyncio.gather(
        fetch_data(MAKES_API_URL.format(make=make), session),
        fetch_data(TYPES_API_URL.format(make=make), session),
    )
    return {
        "make": make,
        "models": [m["Model_Name"] for m in models] if models else None,
        "vehicle_types": sorted({t["VehicleTypeName"] for t in types}) if types else None,
    }


async def collect_data():
    async with aiohttp.ClientSession() as session:
        cars = await asyncio.gather(*(get_car_info(make, session) for make in CAR_MAKES))
    return {"timestamp": datetime.now().isoformat(), "cars": cars}


def save_data(data):
    filename = f"cars_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    return filename


def print_data(data):
    for car in data["cars"]:
        print(f"\nМарка: {car['make'].upper()}")
        if car["models"] is None:
            print("  Модели: не удалось получить")
        else:
            print(f"  Моделей: {len(car['models'])}, например: {', '.join(car['models'][:5])}")
        print(f"  Типы: {', '.join(car['vehicle_types']) if car['vehicle_types'] else 'не удалось получить'}")


async def main():
    for run in range(1, RUNS + 1):
        print(f"\n===== Сбор данных {run} из {RUNS} =====")
        data = await collect_data()
        if all(car["models"] is None and car["vehicle_types"] is None for car in data["cars"]):
            print("Не удалось получить данные")
            return

        print_data(data)
        try:
            print(f"\nДанные сохранены в {save_data(data)}")
        except OSError as e:
            print(f"Ошибка записи файла: {e}")
            return

        if run < RUNS:
            await asyncio.sleep(INTERVAL)

    print("\nВсе сборы выполнены")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nфырк")
