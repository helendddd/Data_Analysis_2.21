#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Использовать словарь, содержащий следующие ключи: фамилия и инициалы; номер
# группы; успеваемость (список из 5 элементов). Написать программу, выполняющую
# следующее: ввод с клавиатуры данных в список; записи должны быть упорядочены
# по алфавиту; вывод на дисплей фамилий и номеров групп для всех студентов,
# имеющих хотя бы одну оценку 2; если таких студентов
# нет, вывести соответствующее сообщение.

# Данные о людях хранятся в файле, создаваемом при помощи SQLite3 – people.db.
# По умолчанию, файл создаётся в домашнем каталоге пользователя.
# В данном файле имеется две таблицы – people и surnames'''


import argparse
import sqlite3
import typing as t
from pathlib import Path


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


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Создать таблицу с информацией о группах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_number TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о студентах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_name TEXT NOT NULL,
            group_id INTEGER NOT NULL,
            performance TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(group_id)
        )
        """
    )

    conn.close()


def add_student(
    database_path: Path, name: str, group: str, performance: t.List[int]
) -> None:
    """
    Добавить студента в базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Получить идентификатор группы в базе данных.
    # Если такой записи нет, то добавить информацию о новой группе.
    cursor.execute(
        """
        SELECT group_id FROM groups WHERE group_number = ?
        """,
        (group,),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO groups (group_number) VALUES (?)
            """,
            (group,),
        )
        group_id = cursor.lastrowid

    else:
        group_id = row[0]

    # Конвертировать список успеваемости в строку
    performance_str = ",".join(map(str, performance))

    # Добавить информацию о новом студенте.
    cursor.execute(
        """
        INSERT INTO students (student_name, group_id, performance)
        VALUES (?, ?, ?)
        """,
        (name, group_id, performance_str),
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех студентов.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT students.student_name, groups.group_number, students.performance
        FROM students
        INNER JOIN groups ON groups.group_id = students.group_id
        """
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "name": row[0],
            "group": row[1],
            "performance": list(map(int, row[2].split(","))),
        }
        for row in rows
    ]


def find(
    database_path: Path,
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех студентов с оценкой 2.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT students.student_name, groups.group_number, students.performance
        FROM students
        INNER JOIN groups ON groups.group_id = students.group_id
        WHERE '2' IN (students.performance)
        """
    )
    rows = cursor.fetchall()

    conn.close()
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
        default=str(Path.home() / "students.db"),
        help="The database file name",
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

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить студента.
    if args.command == "add":
        add_student(db_path, args.name, args.group, args.performance)

    # Отобразить всех студентов.
    elif args.command == "display":
        display_students(select_all(db_path))

    # Выбрать требуемых студентов.
    elif args.command == "find":
        display_students(find(db_path))
        pass


if __name__ == "__main__":
    main()
