import json
import sys
import keyring
import keyring.errors

from orpheusplus import USER_PATH


class UserManager():
    def __init__(self):
        self.info = self.load_user()

    def load_user(self):
        # Load from .meta/user
        try:
            with open(USER_PATH, "r") as f:
                user = json.load(f)
            user["passwd"] = keyring.get_password("orpheusplus", user["user"])
        except:
            self._handle_user_not_exist()

        return user

    @staticmethod
    def save_user(database, user, passwd):
        # Save to .meta/user
        with open(USER_PATH, "w") as f:
            json.dump({"database": database, "user": user}, f)
        keyring.set_password("orpheusplus", user, passwd)

    @staticmethod
    def remove_user(user):
        with open(USER_PATH, "w") as f:
            json.dump({}, f)
        try:
            keyring.delete_password("orpheusplus", user)
        except keyring.errors.PasswordDeleteError:
            pass

    def _handle_user_not_exist(self):
        sys.exit(
            "User info incorrect. Please make sure the user exists in MySQL "
            "and run `orpheusplus config` to reconfigure the user info."
        )
