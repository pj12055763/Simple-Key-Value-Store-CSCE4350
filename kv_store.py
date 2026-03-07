#!/usr/bin/env python3
"""
Simple persistent key-value store.

Commands:
  SET <key> <value>
  GET <key>
  EXIT
"""

import os
import sys

DATA_FILE = "data.db"

try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
except AttributeError:
    pass


class Node:
    def __init__(self, key: str, value: str, next_node=None):
        self.key = key
        self.value = value
        self.next = next_node


class LinkedListIndex:
    def __init__(self):
        self.head = None

    def set(self, key: str, value: str) -> None:
        current = self.head
        while current is not None:
            if current.key == key:
                current.value = value
                return
            current = current.next
        self.head = Node(key, value, self.head)

    def get(self, key: str):
        current = self.head
        while current is not None:
            if current.key == key:
                return current.value
            current = current.next
        return None


class KeyValueStore:
    def __init__(self, path: str):
        self.path = path
        self.index = LinkedListIndex()
        self.db_file = None

    def open(self) -> None:
        if not os.path.exists(self.path):
            open(self.path, "a", encoding="utf-8").close()

        self._replay_log()
        self.db_file = open(self.path, "a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        if self.db_file is not None:
            self.db_file.flush()
            os.fsync(self.db_file.fileno())
            self.db_file.close()
            self.db_file = None

    def _escape(self, text: str) -> str:
        return (
            text.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\n", "\\n")
        )

    def _unescape(self, text: str) -> str:
        result = []
        i = 0

        while i < len(text):
            if text[i] == "\\" and i + 1 < len(text):
                nxt = text[i + 1]
                if nxt == "n":
                    result.append("\n")
                elif nxt == "t":
                    result.append("\t")
                elif nxt == "\\":
                    result.append("\\")
                else:
                    result.append(nxt)
                i += 2
            else:
                result.append(text[i])
                i += 1

        return "".join(result)

    def _replay_log(self) -> None:
        with open(self.path, "r", encoding="utf-8") as file:
            for raw_line in file:
                line = raw_line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t", 2)
                if len(parts) != 3:
                    continue

                command, raw_key, raw_value = parts
                if command != "SET":
                    continue

                key = self._unescape(raw_key)
                value = self._unescape(raw_value)
                self.index.set(key, value)

    def set(self, key: str, value: str) -> None:
        escaped_key = self._escape(key)
        escaped_value = self._escape(value)

        self.db_file.write(f"SET\t{escaped_key}\t{escaped_value}\n")
        self.db_file.flush()
        os.fsync(self.db_file.fileno())

        self.index.set(key, value)

    def get(self, key: str):
        return self.index.get(key)


def process_command(store: KeyValueStore, line: str) -> bool:
    line = line.strip()

    if not line:
        return True

    parts = line.split(None, 1)
    command = parts[0].upper()

    if command == "EXIT":
        return False

    if command == "SET":
        if len(parts) < 2:
            return True

        rest = parts[1]
        kv_parts = rest.split(None, 1)

        if len(kv_parts) == 2 and kv_parts[0]:
            key, value = kv_parts
            store.set(key, value)
            print("OK", flush=True)

        return True

    if command == "GET":
        if len(parts) < 2:
            return True

        key = parts[1].strip()
        if key:
            value = store.get(key)
            if value is not None:
                print(value, flush=True)

        return True

    return True


def main() -> int:
    store = KeyValueStore(DATA_FILE)

    try:
        store.open()
        while True:
            line = sys.stdin.readline()
            if line == "":
                break
            if not process_command(store, line):
                break
    finally:
        store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())