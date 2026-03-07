#!/usr/bin/env python3
"""
Simple persistent key-value store.

Supports:
  SET <key> <value>
  GET <key>
  EXIT

Requirements satisfied:
- CLI via STDIN / STDOUT
- append-only persistence to data.db
- replay log on startup
- custom in-memory index (no built-in dict/map)
- last write wins
"""

import os
import sys


DATA_FILE = "data.db"
MISSING_VALUE_RESPONSE = "NOT_FOUND"


class Node:
    """Linked-list node for the custom in-memory index."""

    def __init__(self, key: str, value: str, next_node=None):
        self.key = key
        self.value = value
        self.next = next_node


class LinkedListIndex:
    """
    Very simple custom index:
    - stores key/value pairs in a singly linked list
    - GET scans linearly
    - SET updates existing key or inserts at head
    """

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
    """Persistent append-only key-value store."""

    def __init__(self, path: str):
        self.path = path
        self.index = LinkedListIndex()
        self.db_file = None

    def open(self) -> None:
        """Open the database file, replay log, then reopen for appending."""
        # Ensure file exists before reading
        if not os.path.exists(self.path):
            open(self.path, "a", encoding="utf-8").close()

        self._replay_log()

        # Line-buffered append mode for writes
        self.db_file = open(self.path, "a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        """Close the database file safely."""
        if self.db_file is not None:
            self.db_file.flush()
            os.fsync(self.db_file.fileno())
            self.db_file.close()
            self.db_file = None

    def _escape(self, text: str) -> str:
        """Escape backslashes, tabs, and newlines for log storage."""
        return (
            text.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
        )

    def _unescape(self, text: str) -> str:
        """Reverse escaping from log storage."""
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
        """Replay append-only log to rebuild the in-memory index."""
        with open(self.path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t", 2)
                if len(parts) != 3:
                    # Ignore malformed lines
                    continue

                command, raw_key, raw_value = parts

                if command != "SET":
                    continue

                key = self._unescape(raw_key)
                value = self._unescape(raw_value)
                self.index.set(key, value)

    def set(self, key: str, value: str) -> None:
        """Persist SET immediately, then update in-memory index."""
        escaped_key = self._escape(key)
        escaped_value = self._escape(value)

        self.db_file.write(f"SET\t{escaped_key}\t{escaped_value}\n")
        self.db_file.flush()
        os.fsync(self.db_file.fileno())

        self.index.set(key, value)

    def get(self, key: str):
        """Return value for key or None if missing."""
        return self.index.get(key)


def process_command(store: KeyValueStore, line: str) -> bool:
    """
    Process one command line.
    Returns False if the program should exit, otherwise True.
    """
    line = line.strip()

    if not line:
        return True

    upper_line = line.upper()

    if upper_line == "EXIT":
        return False

    if upper_line.startswith("SET "):
        # Allow values to contain spaces by splitting only twice
        parts = line.split(" ", 2)

        if len(parts) < 3 or not parts[1]:
            print("ERROR")
            return True

        key = parts[1]
        value = parts[2]
        store.set(key, value)
        print("OK")
        return True

    if upper_line.startswith("GET "):
        parts = line.split(" ", 1)

        if len(parts) < 2 or not parts[1].strip():
            print("ERROR")
            return True

        key = parts[1].strip()
        value = store.get(key)

        if value is None:
            print(MISSING_VALUE_RESPONSE)
        else:
            print(value)

        return True

    print("ERROR")
    return True


def main() -> int:
    store = KeyValueStore(DATA_FILE)

    try:
        store.open()

        for line in sys.stdin:
            should_continue = process_command(store, line)
            if not should_continue:
                break

    finally:
        store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())