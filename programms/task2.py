#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Использовать словарь, содержащий следующие ключи: фамилия и инициалы; номер
# группы; успеваемость (список из 5 элементов). Написать программу, выполняющую
# следующее: ввод с клавиатуры данных в список; записи должны быть упорядочены
# по алфавиту; вывод на дисплей фамилий и номеров групп для всех студентов,
# имеющих хотя бы одну оценку 2; если таких студентов
# нет, вывести соответствующее сообщение.

# Для своего варианта лабораторной работы 2.17
# необходимо реализовать хранение данных в базе
# данных pgsql. Информация в базе данных
# должна храниться не менее чем в двух таблицах.


import argparse
import typing as t
import psycopg2


def display_students(students: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список студентов.
    """
    # Проверить, что список студентов не пуст.
    if students:
        # Заголовок таблицы.
        line = "+-{}-+-{}-+-{}-+-{}-+".format(
            "-" * 4, "-" * 30, "-" * 10, "-" * 20
        )
        print(line)
        print(
            "| {:^4} | {:^30} | {:^10} | {:^20} |".format(
                "No", "Ф.И.О.", "Группа", "Успеваемость"
            )
        )
        print(line)

        # Вывести данные о всех студентах.
        for idx, student in enumerate(students, 1):
            print(
                "| {:>4} | {:<30} | {:<10} | {:<20} |".format(
                    idx,
                    student.get("name", ""),
                    student.get("group", ""),
                    ", ".join(map(str, student.get("performance", []))),
                )
            )
            print(line)

    else:
        print("Список студентов пуст.")


def create_db(conn) -> None:
    """
    Создать базу данных.
    """
    with psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="12345",
        host="localhost",
        port=5432,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS groups (
                    group_id SERIAL PRIMARY KEY,
                    group_number VARCHAR(50) NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS students (
                    student_id SERIAL PRIMARY KEY,
                    student_name VARCHAR(255) NOT NULL,
                    group_id INTEGER NOT NULL REFERENCES groups(group_id),
                    performance TEXT NOT NULL
                )
                """
            )
            conn.commit()
    cursor.close()


def add_student(conn, name: str, group: str, performance: t.List[int]) -> None:
    """
    Добавить студента в базу данных.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="12345",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()

    # Получить идентификатор группы в базе данных.
    # Если такой записи нет, то добавить информацию о новой группе.
    cursor.execute(
        """
        SELECT group_id FROM groups WHERE group_number = %s
        """,
        (group,),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO groups (group_number) VALUES (%s) RETURNING group_id
            """,
            (group,),
        )
        group_id = cursor.fetchone()[0]
    else:
        group_id = row[0]

    # Конвертировать список успеваемости в строку
    performance_str = ",".join(map(str, performance))

    # Добавить информацию о новом студенте.
    cursor.execute(
        """
        INSERT INTO students (student_name, group_id, performance)
        VALUES (%s, %s, %s)
        """,
        (name, group_id, performance_str),
    )

    conn.commit()
    cursor.close()


def find(students: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Выбрать всех студентов с оценкой 2 из базы данных PostgreSQL.
    """
    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="12345",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()

    # Выполнение SQL-запроса для выбора студентов с оценкой 2
    cursor.execute(
        """
        SELECT students.student_name, groups.group_number, students.performance
        FROM students
        INNER JOIN groups ON groups.group_id = students.group_id
        WHERE '2' = ANY(string_to_array(students.performance, ','))
        """
    )
    rows = cursor.fetchall()

    # Закрытие соединения с базой данных
    cursor.close()
    conn.close()

    # Форматирование результатов запроса в словари
    return [
        {
            "name": row[0],
            "group": row[1],
            "performance": list(map(int, row[2].split(","))),
        }
        for row in rows
    ]


def select_all(conn) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех студентов.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="12345",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT students.student_name, groups.group_number, students.performance
        FROM students
        INNER JOIN groups ON groups.group_id = students.group_id
        """
    )
    rows = cursor.fetchall()

    cursor.close()
    return [
        {
            "name": row[0],
            "group": row[1],
            "performance": list(map(int, row[2].split(","))),
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        help="The database connection string",
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("students")
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления студента.
    add = subparsers.add_parser(
        "add", parents=[file_parser], help="Add a new student"
    )
    add.add_argument(
        "-n",
        "--name",
        action="store",
        required=True,
        help="The student's name",
    )
    add.add_argument(
        "-g", "--group", action="store", help="The student's group"
    )
    add.add_argument(
        "-p",
        "--performance",
        action="store",
        nargs=5,
        type=int,
        required=True,
        help="The student's performance (5 grades)",
    )

    # Создать субпарсер для отображения всех студентов.
    _ = subparsers.add_parser(
        "display", parents=[file_parser], help="Display all students"
    )

    # Создать субпарсер для выбора студентов.
    _ = subparsers.add_parser(
        "find", parents=[file_parser], help="Select the students with 2"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Подключиться к базе данных.
    conn = psycopg2.connect(args.db)
    create_db(conn)

    # Добавить студента.
    if args.command == "add":
        add_student(conn, args.name, args.group, args.performance)

    # Отобразить всех студентов.
    elif args.command == "display":
        display_students(select_all(conn))

        # Выбрать требуемых рааботников.
    elif args.command == "find":
        display_students(find(conn))
        pass

    # Закрыть соединение.
    conn.close()


if __name__ == "__main__":
    main()
