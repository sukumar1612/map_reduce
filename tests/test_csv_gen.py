import csv

from faker import Faker

if __name__ == "__main__":
    fake = Faker()
    number_of_records = int(5000000)

    with open("random_data_5.csv", mode="w") as file:
        file_writer = csv.writer(
            file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )

        file_writer.writerow(["name", "item_type", "quantity", "age"])

        for _ in range(number_of_records):
            file_writer.writerow(
                [
                    fake.name(),
                    fake.random_choices(
                        elements=tuple(["apple", "orange", "banana"]), length=1
                    )[0],
                    fake.numerify("@###"),
                    fake.numerify("@#"),
                ]
            )
