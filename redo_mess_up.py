import json

# Read the metadata file
with open("metadata.json", "r") as meta_file:
    metadata = json.load(meta_file)

# Initialize the database with the initial values from metadata
database = {}
for column, values in metadata["table"].items():
    database[column] = values[0]

# Read the log file
with open("log.txt", "r") as log_file:
    lines = log_file.readlines()

# Initialize variables to track active transactions
active_transactions = set()
checkpoint_active = False

# Process the log file
for line in lines:
    line = line.strip()
    parts = line.split()

    if parts[0] == "<start":
        transaction = parts[1]
        active_transactions.add(transaction)
    elif parts[0] == "<commit":
        transaction = parts[1]
        active_transactions.remove(transaction)
    elif parts[0] == "<START":
        checkpoint_active = True
    elif parts[0] == "<END":
        checkpoint_active = False
    else:
        if not checkpoint_active:
            transaction, row, column, old_value = parts
            old_value = int(old_value)
            # Perform UNDO: revert the column to the old value
            database[column] = old_value
        else:
            transaction, row, column, new_value = parts
            new_value = int(new_value)
            # Update the database with the new value
            database[column] = new_value

# Print transactions that performed UNDO
for transaction in active_transactions:
    print(f"Transaction {transaction} performed UNDO")

# Print the value of variables in the database
print(json.dumps({"table": database}, indent=4))
