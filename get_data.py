# get_data.py — PG-only runner
import os
from module_trafficsource import *
from module_content import *
from module_overall import *


def main():
    if not os.path.exists(CREDENTIALS_FOLDER):
        print(f"Credentials folder '{CREDENTIALS_FOLDER}' does not exist.")
        return

    os.makedirs(TOKEN_FOLDER, exist_ok=True)

    files = [f for f in os.listdir(CREDENTIALS_FOLDER) if f.endswith(".json")]
    if not files:
        print(f"No credentials files found in {CREDENTIALS_FOLDER}.")
        return

    token_files = [f for f in os.listdir(TOKEN_FOLDER) if f.endswith(".pickle")]

    if len(files) == len(token_files):
        print("All credentials have tokens. Processing all accounts...")
        for cred_file in files:
            process_one(cred_file)
            process_content(files[choice])
            process_overall(files[choice])
    else:
        print("Available credentials files:")
        for i, file in enumerate(files):
            print(f"{i + 1}. {file}")
        try:
            choice = int(input("Select a file by number: ")) - 1
            if choice < 0 or choice >= len(files):
                print("Invalid choice.")
                return
            process_one(files[choice])
            process_content(files[choice])
            process_overall(files[choice])
        except ValueError:
            print("Invalid input. Please enter a number.")

if __name__ == "__main__":
    main()
