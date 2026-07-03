"""
Create or update a login user (writes bcrypt hashes to users.json).

Usage:
  python make_user.py <username> <password>

Login auth turns on automatically once users.json (or the AUTH_USERS env var)
contains at least one user. Delete users.json to disable login again.
"""

import json
import os
import sys

import bcrypt

_USERS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    username, password = sys.argv[1], sys.argv[2]
    users = {}
    if os.path.exists(_USERS_PATH):
        with open(_USERS_PATH, "r", encoding="utf-8") as fh:
            users = json.load(fh)
    users[username] = bcrypt.hashpw(password.encode("utf-8")[:72],
                                    bcrypt.gensalt()).decode("ascii")
    with open(_USERS_PATH, "w", encoding="utf-8") as fh:
        json.dump(users, fh, indent=2)
    print(f"Saved user '{username}' to {_USERS_PATH} ({len(users)} user(s) total).")


if __name__ == "__main__":
    main()
