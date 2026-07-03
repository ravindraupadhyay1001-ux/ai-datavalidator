"""
Create or update a login user (writes bcrypt hashes to users.json).

Usage:
  python make_user.py <username> <password> [role]

  role is one of: admin, analyst, readonly (default: analyst)

Login auth turns on automatically once users.json (or the AUTH_USERS env var)
contains at least one user. Delete users.json to disable login again.
"""

import json
import os
import sys

import bcrypt

_USERS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")
_VALID_ROLES = ("admin", "analyst", "readonly")


def main():
    if len(sys.argv) not in (3, 4):
        print(__doc__)
        sys.exit(1)
    username, password = sys.argv[1], sys.argv[2]
    role = sys.argv[3] if len(sys.argv) == 4 else "analyst"
    if role not in _VALID_ROLES:
        print(f"Invalid role '{role}'. Must be one of: {', '.join(_VALID_ROLES)}")
        sys.exit(1)
    users = {}
    if os.path.exists(_USERS_PATH):
        with open(_USERS_PATH, "r", encoding="utf-8") as fh:
            users = json.load(fh)
    users[username] = {
        "hash": bcrypt.hashpw(password.encode("utf-8")[:72], bcrypt.gensalt()).decode("ascii"),
        "role": role,
    }
    with open(_USERS_PATH, "w", encoding="utf-8") as fh:
        json.dump(users, fh, indent=2)
    print(f"Saved user '{username}' (role={role}) to {_USERS_PATH} ({len(users)} user(s) total).")


if __name__ == "__main__":
    main()
